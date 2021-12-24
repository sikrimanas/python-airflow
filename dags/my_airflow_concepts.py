from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from random import seed, random

#from airflow.utils.helpers import chain, cross_downstreams

# method 1 to define a DAG
#dag = DAG('core_concepts', schedule_interval='@daily', catchup=False)
#op = Op(dag=dag)

# define default arguments
default_arguments = {
    'owner': 'Manas Sikri',
    'start_date': days_ago(1)
}

# method 2 to define a DAG - Context manager
with DAG(
    'core_concepts', 
    schedule_interval='@daily', 
    catchup=False,
    default_args=default_arguments
) as dag:

# Define Task 1 - Bash Task
    bash_task = BashOperator(
        task_id = "bash_command",
        bash_command = "echo $TODAY",
        env = {"TODAY": "2021-12-10"}
    )

    def print_random_number(number):
        seed(number)
        print(random())

# Define Task 2 - Python Task
    python_task = PythonOperator(
        task_id = "python_function",
        python_callable = print_random_number,
        op_args=[1]
    )

# Now set the dependencies

bash_task >> python_task

#bash_task.set_downstream(python_task)

#op1 >> op2 >> op3 >> op4

#chain(op1,op2,op3,op4)

#cross_downstreams([op1,op2],[op3,op4])
#[op1,op2] >> op3
#[op1,op2] >> op4