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
    'practica_SSITL_dag',
    default_args=default_args,
    description='Practica SSITL',
    schedule_interval='@weekly',
    tags=['Practica', 'Codigo Facilito', 'Flavio Sandoval']
) as dag:
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    
    extract_inventario = DummyOperator(task_id='extract_inventario')
    extract_compras = DummyOperator(task_id='extract_compras')
    extract_ventas = DummyOperator(task_id='extract_ventas')
    
    join_get_predictions = DummyOperator(task_id='join_get_predictions')
    
    ingest_data = DummyOperator(task_id='ingest_data')
    alert_data = DummyOperator(task_id='alert_data')
    
    start >> [extract_inventario, extract_compras, extract_ventas] >> join_get_predictions >> [ingest_data, alert_data] >> end