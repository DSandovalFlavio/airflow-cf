# default airflow imports
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

# Google Cloud provider imports
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# otros imports
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'tags': ['DMS']
}

def format_date(date_str):
    date_parts = date_str.split(' ')[0].split('-')
    date = datetime(int(date_parts[0]), int(date_parts[1]), int(date_parts[2]))
    return date.strftime('%-m/%-d/%Y'), date.strftime('%Y-%m-%d')

def extract_data(ti, sd, ed):
    start_date, start_date_file = format_date(sd)
    end_date, end_date_file = format_date(ed)
    # curl -H "X-API-Key: e83d0120" https://my.api.mockaroo.com/marketing_campaing.json?start_date=1-1-2022&end_date=1-31-2022
    url = f'https://my.api.mockaroo.com/marketing_campaing.json?start_date={start_date}&end_date={end_date}'
    headers = {'X-API-Key': 'd67d54e0'}
    response = requests.get(url, headers=headers)
    tmp_file = f"marketing_campaing_{start_date_file}_{end_date_file}.csv"
    tmp_path = f'/tmp/{tmp_file}'
    with open(tmp_path, 'w') as file:
        file.write(response.text)
    ti.xcom_push(key='tmp_path', value=tmp_path)
    ti.xcom_push(key='tmp_file', value=tmp_file)
    
    
with DAG(
    'elt_dag',
    default_args=default_args,
    description='Proceso para extraer datos de la API de Mockaroo y cargarlos a BigQuery',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 4, 1),
    end_date=datetime(2024, 12, 31),
    catchup=True
) as dag:
    
    # touch points
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    
    download_data = PythonOperator(
        task_id='download_data',
        python_callable=extract_data,
        provide_context=True,
        op_kwargs={
            'sd': '{{ data_interval_start }}',
            'ed': '{{ data_interval_end }}'
        }
        
    )
    
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src='{{ ti.xcom_pull(key="tmp_path") }}',
        dst='{{ ti.xcom_pull(key="tmp_file") }}',
        bucket='airflow-data-dms',
        gcp_conn_id='GCP_Default'
    )
    
    send_to_bq = GCSToBigQueryOperator(
        task_id='send_to_bq',
        bucket='airflow-data-dms',
        source_objects=['{{ ti.xcom_pull(key="tmp_file") }}'],
        destination_project_dataset_table='dms_airflow.marketing_campaing',
        source_format='CSV',
        gcp_conn_id='GCP_Default',
        write_disposition='WRITE_APPEND',
    )
    
    # test
    start >> download_data >> upload_to_gcs >> send_to_bq >> end
    