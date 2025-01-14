FROM quay.io/astronomer/astro-runtime:12.4.0

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate

    ENV PYTHONPATH="/usr/local/airflow:/usr/local/airflow/dags:/app/src/models"
