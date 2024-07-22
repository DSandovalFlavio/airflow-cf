# imports airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# imports other libraries
from datetime import datetime, timedelta

# default arguments
default_args = {
    'owner': 'Codigo Facilito Team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# create a DAG object
with DAG(
    'practica_SRP_dag',
    default_args=default_args,
    description='Practica SRP',
    schedule_interval='@monthly',
    tags=['Practica', 'Codigo Facilito', 'Flavio Sandoval']
) as dag:
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    
    extract_internal_db = DummyOperator(task_id='extract_internal_db')
    extract_external_api = DummyOperator(task_id='extract_external_api')
    
    join_get_predictions = DummyOperator(task_id='join_get_predictions')
    
    send_email = DummyOperator(task_id='send_email')
    
    start >> [extract_internal_db, extract_external_api] >> join_get_predictions >> send_email >> end