import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from cosmos import DbtDag, ProfileConfig, ExecutionConfig, ProjectConfig, DbtRunDockerOperator, DbtTaskGroup
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

from include.data.collect_data.runners.run_all import collect_recent_gw
from include.data.utils.access_s3_bucket import update_recent_files, download_files
from include.data.utils.access_snowflake import upload_new_data
from include.models.predict_model import predict_values
from include.models.custom_components.transformers import ColumnDropper, SkewNormalizationTransformer, IsolationForestTransformer
from include.models.custom_components.metrics import output1_rmse, output1_mae, output2_rmse, output2_mae, combined_metric
from include.models.custom_components.utils import CustomTimeSeriesCV, fix_train_test_split

default_args = {
    'owner': 'Sami',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(hours=12),
}

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_connection",
        profile_args={"database": "fpl_database", "schema": "dbt_schema"},
    )
)

def get_current_season_task(**kwargs):
    from include.data.collect_data.runners.run_all import get_current_season
    return get_current_season()


with DAG(
    dag_id= 'gameweek_dag',
    default_args=default_args,
    description='A DAG to collect and update data',
    schedule_interval='@weekly',  # Adjust the schedule as needed
    start_date=datetime(2024, 12, 11),
    catchup=False,
    is_paused_upon_creation= False
) as dag:

    get_season_task = PythonOperator(
        task_id="get_current_season",
        python_callable=get_current_season_task,
    )

    collect_needed_s3_files = PythonOperator(
        task_id="collect_needed_s3_files",
        python_callable=download_files,
        op_kwargs={
            "files_to_download": ["fbref_ids.csv", "player_compiled_ids.csv"],
            "season": "{{ task_instance.xcom_pull(task_ids='get_current_season') }}"
        },
    )

    collect_gw_task = PythonOperator(
        task_id='collect_recent_gw',
        python_callable=collect_recent_gw,
    )

    update_s3_task = PythonOperator(
        task_id='update_recent_files',
        python_callable=update_recent_files,
        op_kwargs={
            "season": "{{ task_instance.xcom_pull(task_ids='get_current_season') }}"
        },
    )

    upload_to_snowflake_task = PythonOperator(
        task_id="upload_data_to_snowflake",
        python_callable=upload_new_data,
    )

    dbt_run_task = DbtTaskGroup(
        group_id="dbt_transformations",
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
        ),
        project_config=ProjectConfig(
            "/usr/local/airflow/dags/dbt_pipeline"
        ),
    )
    
    collect_predictions_task = PythonOperator(
        task_id=  "upload_predictions_to_snowflake",
        python_callable=predict_values
    )

    dbt_predictions_transformations = DbtTaskGroup(
        group_id="dbt_predictions_transformations",
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
        ),
        project_config=ProjectConfig(
            "/usr/local/airflow/dags/dbt_pipeline"
        ),
    )
    # Define dependencies
    get_season_task >> collect_needed_s3_files
    collect_needed_s3_files >> collect_gw_task
    collect_gw_task >> update_s3_task
    update_s3_task >> upload_to_snowflake_task
    upload_to_snowflake_task >> dbt_run_task
    dbt_run_task >> collect_predictions_task
    collect_predictions_task >> dbt_predictions_transformations