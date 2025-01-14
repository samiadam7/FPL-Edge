from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import joblib

from include.models.train_model import retrain_model, monitor_model_performance
from include.models.custom_components.transformers import ColumnDropper, SkewNormalizationTransformer, IsolationForestTransformer
from include.models.custom_components.metrics import output1_rmse, output1_mae, output2_rmse, output2_mae, combined_metric
from include.models.custom_components.utils import CustomTimeSeriesCV, fix_train_test_split

MODEL_FILEPATH = "/usr/local/airflow/include/models/fpl_player_performance_model.pkl"
PERFORMANCE_THRESHOLD = 0.53

default_args = {
    'owner': 'Sami',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='retrain_model_dag',
    default_args=default_args,
    description='DAG to monitor and retrain ML model',
    schedule_interval="@monthly",
    start_date=datetime(2024, 12, 1),
    catchup=False,
) as dag:

    def check_model_performance(**kwargs):
        needs_retrain = monitor_model_performance(MODEL_FILEPATH, threshold=PERFORMANCE_THRESHOLD)
        kwargs['ti'].xcom_push(key='needs_retrain', value=needs_retrain)

    monitor_task = PythonOperator(
        task_id='monitor_model_performance',
        python_callable=check_model_performance,
        provide_context=True,
    )

    def retrain(**kwargs):
        needs_retrain = kwargs['ti'].xcom_pull(key='needs_retrain', task_ids='monitor_model_performance')
        if needs_retrain:
            retrain_model(MODEL_FILEPATH)

    retrain_task = PythonOperator(
        task_id='retrain_model',
        python_callable=retrain,
        provide_context=True,
    )

    monitor_task >> retrain_task
