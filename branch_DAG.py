# BrachPythonOperator
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import random

default_args = {
    'owner':'Codigo Facilito Team',
    'start_date': datetime(2024, 5, 1),
    'depends_on_past': False,
    'email_on_failture': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=20)
}

def _extract():
    print('Extracting data')
    print('Counting rows')
    return random.randint(1, 10)

def _branch(**kwargs):
    ti = kwargs['ti']
    rows = ti.xcom_pull(task_ids='extract')
    if rows > 5:
        return 'transform1'
    return 'predict_data_faltante'

with DAG(
    'DAG_Branch',
    default_args = default_args,
    description='Ejemplo de Sensor',
    schedule_interval='@daily',
    tags=['Ingenieria']
) as dag:
    start = DummyOperator(task_id='start')
    
    extract = PythonOperator(
        task_id='extract',
        python_callable=_extract
    )
    
    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=_branch
    )
    
    transform1 = DummyOperator(task_id='transform1')
    
    predict_data_faltante = DummyOperator(task_id='predict_data_faltante')
    transform2 = DummyOperator(task_id='transform2')
    
    ingest = DummyOperator(task_id='ingest',
                           trigger_rule='one_success')
    
    end = DummyOperator(task_id='end')
    
    start >> extract >> branch_task
    branch_task >> transform1
    branch_task >> predict_data_faltante
    transform1 >> ingest
    predict_data_faltante >> transform2 >> ingest
    ingest >> end
        