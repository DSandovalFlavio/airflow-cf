from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner':'Codigo Facilito Team',
    'start_date': datetime(2024, 5, 1),
    'depends_on_past': False,
    'email_on_failture': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=20)
}

with DAG(
    'DAG_Ventas',
    default_args = default_args,
    description='Ejemplo de Sensor',
    schedule_interval='@daily',
    tags=['Ingenieria']
) as dag:
    
    start = DummyOperator(task_id='start')
    
    extract = DummyOperator(task_id='extract')
    
    transform1 = DummyOperator(task_id='transform1')
    transform2 = BashOperator(
        task_id='transform2',
        bash_command='sleep 5'
    )
    
    ingest1 = DummyOperator(task_id='ingest1')
    ingest2 = DummyOperator(task_id='ingest2')
    
    end = DummyOperator(task_id='end')
    
    start >> extract >> [transform1, transform2]
    transform1 >> ingest1
    transform2 >> ingest2
    [ingest1, ingest2] >> end