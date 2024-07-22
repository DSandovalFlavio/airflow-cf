from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta

default_args = {
    'owner': 'CodigoFacilito',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'tags': ['etl']
}

with DAG('ETL_PostgreSQL', 
         default_args=default_args,
         description='Extracción, transformación y carga de datos en PostgreSQL',
         schedule_interval=None) as dag:

    def _extract():
        import requests
        url = 'https://my.api.mockaroo.com/sales_db.json'
        headers = {'X-API-Key': 'd67d54e0'}
        response = requests.get(url, headers=headers)
        with open('/tmp/sales_db_py.csv', 'wb') as file:
            file.write(response.content)
            file.close()
    
    def _transform():
        import pandas as pd
        df_py = pd.read_csv('/tmp/sales_db_py.csv')
        df_bash = pd.read_csv('/tmp/sales_db_bash.csv')
        df = pd.concat([df_py, df_bash], ignore_index=True)
        df = df.groupby(['date', 'store'])['sales'].sum().reset_index()
        df = df.rename(columns={'date':'ddate'})
        df.to_csv('/tmp/sales_db.csv', sep='\t', index=False, header=False)
        print(df.head())
    
    def _load_data():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg_hook = PostgresHook(postgres_conn_id='postgres_local_conn')
        pg_hook.bulk_load(table="sales_db", tmp_file='/tmp/sales_db.csv')
    
    extract_data_py = PythonOperator(
        task_id='extract_data_py',
        python_callable=_extract
    )
    
    extrac_data_bash = BashOperator(
        task_id='extract_data_bash',
        bash_command='curl -H "X-API-Key: d67d54e0" https://my.api.mockaroo.com/sales_db.json > /tmp/sales_db_bash.csv'
    )
    
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=_transform
    )
    
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_local_conn',
        sql='sql/create_table.sql'
    )
    
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=_load_data
    )
    
    [extract_data_py, extrac_data_bash] >> transform_data >> create_table >> load_data
    
            