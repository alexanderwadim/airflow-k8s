from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define a Python function that will be called by the PythonOperator
def print_hello():
    return 'Hello Airflow!'

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 27),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Instantiate the DAG object
dag = DAG(
    'test_dag',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval='@daily',
)

# Define tasks
start_task = DummyOperator(task_id='start_task', dag=dag)

hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)

end_task = DummyOperator(task_id='end_task', dag=dag)

# Define task dependencies
start_task >> hello_task >> end_task
