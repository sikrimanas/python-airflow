import unittest
from unittest.mock import patch

from datetime import datetime

from airflow import DAG
from airflow.utils.state import State
from airflow.models import TaskInstance
from airflow.operators.bigquery_plugins import TestBigQueryDataValidationOperator

from airflow.exceptions import AirflowExceptions

def mock_runquery():
    def return_empty_lsit(*args, **kwargs):
        return []

class TestBigQueryDataValidationOperator(unittest.TestCase):

    def setUp(self):

        EXEC_DATE = '2021-12-26'

        self.dag = DAG(
                'test_big_query_data_validation', 
                schedule_interval= '#daily'
                default_args = {
                    'start_date' = EXEC_DATE
                }
            )

        # operator
        self.op = BigQueryDataValidationOperator(
            task_id = 'bigquery_op',
            sql = 'SELECT COUNT(*) FROM `example.example.example`',
            location = 'europe-west2',
            dag =self.dag
        )

        # task instance
        self.ti= TaskInstance(
            task = self.op, 
            execution_date = datetime.strftime(EXEC_DATE, '%Y-%m-%d')
        )

    @patch.object(
        BigQueryDataValidationOperator , 
        'run_query', 
        new_callable=mock_runquery
        )

    def test_with_empty_results(self):
        with self.assertRaise(AirflowExcetion) as context:
            self.ti.run()

        self.assertEqual(self.ti.state, State.FAILED)
        self.assertEqual(str(context.exception), "Query returned no results")




