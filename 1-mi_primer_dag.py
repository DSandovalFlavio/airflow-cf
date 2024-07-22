# airflow imports
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# otros imports
import requests
import pandas as pd

default_args = {
    'owner': 'Codigo Facilito Team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Funcion para extraer datos de la API de Mockaroo
def _extract_data():
    # curl -H "X-API-Key: e83d0120" https://my.api.mockaroo.com/marketing_campaing.json
    url = 'https://my.api.mockaroo.com/marketing_campaing.json'
    headers = {'X-API-Key': 'e83d0120'}
    response = requests.get(url, headers=headers)
    tmp_path = '/tmp/python_marketing_campaing.csv'
    with open(tmp_path, 'w') as file:
        file.write(response.text)
        
def _join_data():
    df1 = pd.read_csv('/tmp/python_marketing_campaing.csv')
    df2 = pd.read_csv('/tmp/bash_marketing_campaing.csv')
    df = pd.concat([df1, df2])
    df.to_csv('/tmp/marketing_campaing.csv', index=False)

with DAG(
    'mi_primer_etl_dag',
    default_args=default_args,
    description='Proceso para extraer datos de la API de Mockaroo y cargarlos a PostgreSQL',
    tags=['etl', 'Codigo Facilito', 'Flavio Sandoval']
) as dag:
    
    # touch points
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    
    extract_data_python = PythonOperator(
        task_id='extract_data_python',
        python_callable=_extract_data
    )
    
    extract_data_bash = BashOperator(
        task_id='extract_data_bash',
        bash_command='curl -H "X-API-Key: e83d012" https://my.api.mockaroo.com/marketing_campaing.json > /tmp/bash_marketing_campaing.csv'
    )
    
    join_data = PythonOperator(
        task_id='join_data',
        python_callable=_join_data
    )
    
    load_data = BashOperator(
        task_id='load_data',
        bash_command= 'echo "Cargando datos a PostgreSQL"'
    )
    
    start >> [extract_data_python, extract_data_bash] >> join_data >> load_data >> end
    