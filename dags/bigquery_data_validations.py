from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.bigquery_plugin import (
    BigQueryDataValidationOperator,
    BigQueryDataSensor
)

default_arguments = {
    'owner': 'Manas Sikri',
    'start_date': days_ago(1)
}

with DAG(
    'bigquery_data_validation',
    schedule_interval='@daily',
    catchup=False,
    default_args=default_arguments,
    user_defined_macros={'project': 'ms-airflow-python'}
) as dag:

    is_table_empty = BigQueryDataValidationOperator(
        task_id = 'is_table_empty',
        sql = 'SELECT COUNT(*) FROM `{{ project }}.vehicle_analytics.history`',
        location = 'us-east1 (South Carolina)'

    )

    dataset_exists = BigQueryDataSensor(
        task_id='dataset_exists',
        project_id='{{ project }}',
        dataset_id='vehicle_analytics'
    )

dataset_exists >> is_table_empty