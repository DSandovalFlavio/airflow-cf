# how to use ExternalTaskSensor
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
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
    'DAG_Analityca_MKT',
    default_args = default_args,
    description='Ejemplo de Sensor',
    schedule_interval='@daily',
    tags=['Ingenieria']
) as dag:
    
    start = DummyOperator(task_id='start')
    
    sensor_DB_Ventas_Raw = ExternalTaskSensor(
        task_id='sensor',
        external_dag_id='DAG_Ventas',
        external_task_id='transform2',
        allowed_states=['success']
    )
    mkt_data = DummyOperator(task_id='mkt_data')
    
    join_transform = DummyOperator(task_id='join_transform')
    
    ingest = DummyOperator(task_id='ingest')
    
    end = DummyOperator(task_id='end')
    
    start >> [sensor_DB_Ventas_Raw, mkt_data] >> join_transform >> ingest >> end