"""

# Example Spark Dag
This Example dag walks you through the concepts of branching, subdags, and trigger rules.
It creates a Dataproc cluster on google cloud and runs a series of PySpark jobs.

"""

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataProcClusterDeleteOperator
)
from airflow.contrib.operators.python_operator import BranchPyhtonOperator
from airflow.contrib.operators.subdag_operator import SubDagOperator

from airflow.utils.dates import days_ago
from dateime import datetime

from pyspark_subdag import weekday_subdag

default_arguments = {
    'owner': 'Manas Sikri',
    'start_date': days_ago(1)
}

def assess_day(execution_date=None):
    date = datetime.strptime(execution_date, '%Y-%m-%d')

    if date.isoweekday() < 6:
        return 'weekday_analytics'
    else:
        return 'weekend_analytics'


with DAG(
    'bigquery_data_analytics',
    schedule_interval='0 20 * * *', 
    catchup=False,
    default_args=default_arguments) as dag:

    dag.doc_md = __doc__

    create_cluster = DataprocClusterCreateOperator(
        task_id = 'create_cluster',
        project_id='ms-airflow-python',
        cluster_name='spark-cluster-{{ ds_nodash }}',
        num_workers=2,
        storage_bucket='ms-logistics-spark-bucket',
        zone='us-east1 (South Carolina)'
    )

    create_cluster.doc_md = """
        # Create Dataproc Cluster
        This task creates a Dataproc cluster in the project ms-airflow-python.
    """

    weekday_or_weekend = BranchPyhtonOperator(
        task_id = 'weekday_or_weekend',
        python_callable=assess_day,
        op_kwargs={'execution_date': '{{ ds }}'}
    )

    weekend_analytics = DataProcPySparkOperator(
        task_id='weekend_analytics',
        main='gs://ms-logistics-spark-bucket/pyspark/weekend/gas_composition_count.py',
        cluster_name='spark-cluster-{{ ds_nodash }}',
        dataproc_pyspark_jars='gs://spark-lib/bigquery/spark-bigquery-latest.jar'
    )

    weekday_analytics = SubDagOperator(
        task_id='weekday_analytics',
        subdag=weekday_subdag(
            parent_dag='bigquery_data_analytics',
            task_id='weekday_analytics',
            schedule_interval='0 20 * * *',
            default_args=default_arguments
        )
    )

    delete_cluster = DataProcClusterDeleteOperator(
        task_id = 'delete_cluster',
        project_id='ms-airflow-python',
        cluster_name='spark-cluster-{{ ds_nodash }}',
        trigger_rule='all-done'
    )

weekday_or_weekend >> weekday_analytics
create_cluster >> weekday_or_weekend >> [weekend_analytics,weekday_analytics] >> delete_cluster