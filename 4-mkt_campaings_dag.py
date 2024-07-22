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
    'owner': 'Codigo Facilito Team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


def _extract_data_fb():
    url = 'https://my.api.mockaroo.com/marketing_campaing.json'
    headers = {'X-API-Key': 'e83d0120'}
    response = requests.get(url, headers=headers)
    tmp_path = '/tmp/marketing_campaing_fb.csv'
    with open(tmp_path, 'w') as file:
        file.write(response.text)
    
def _extract_data_gads():
    # curl -H "X-API-Key: e83d0120" https://my.api.mockaroo.com/marketing_campaing.json
    url = 'https://my.api.mockaroo.com/marketing_campaing.json'
    headers = {'X-API-Key': 'e83d0120'}
    response = requests.get(url, headers=headers)
    tmp_path = '/tmp/marketing_campaing_gads.csv'
    with open(tmp_path, 'w') as file:
        file.write(response.text)

def _extract_data_ytads():
    # curl -H "X-API-Key: e83d0120" https://my.api.mockaroo.com/marketing_campaing.json
    url = 'https://my.api.mockaroo.com/marketing_campaing.json'
    headers = {'X-API-Key': 'e83d0120'}
    response = requests.get(url, headers=headers)
    tmp_path = '/tmp/marketing_campaing_ytads.csv'
    with open(tmp_path, 'w') as file:
        file.write(response.text)
    
    
with DAG(
    'mkt_campaings_dag',
    default_args=default_args,
    description='ELT para cargar datos de campañas de marketing a BigQuery',
    schedule_interval='@daily',
    start_date=datetime(2024, 3, 30),
    tags = ['Codigo Facilito', 'Flavio Sandoval', 'GCP'],
    catchup=False
) as dag:
    
    # touch points
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    
    extract_data_fb = PythonOperator(
        task_id='extract_data_fb',
        python_callable=_extract_data_fb
    )
    
    extract_data_gads = PythonOperator(
        task_id='extract_data_gads',
        python_callable=_extract_data_gads
    )
    
    extract_data_ytads = PythonOperator(
        task_id='extract_data_ytads',
        python_callable=_extract_data_ytads
    )
    
    upload_to_gcs_fb = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs_fb',
        src='/tmp/marketing_campaing_fb.csv',
        dst='marketing_campaing_fb.csv',
        bucket='cf-data-mkt',
        gcp_conn_id='google-connection'
    )
    
    upload_to_gcs_gads = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs_gads',
        src='/tmp/marketing_campaing_gads.csv',
        dst='marketing_campaing_gads.csv',
        bucket='cf-data-mkt',
        gcp_conn_id='google-connection'
    )
    
    upload_to_gcs_ytads = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs_ytads',
        src='/tmp/marketing_campaing_ytads.csv',
        dst='marketing_campaing_ytads.csv',
        bucket='cf-data-mkt',
        gcp_conn_id='google-connection'
    )
    
    schema_reports = [
        {'name': 'date', 'type': 'DATE'},
        {'name': 'campaign', 'type': 'STRING'},
        {'name': 'clicks', 'type': 'INTEGER'},
        {'name': 'views', 'type': 'INTEGER'},
        {'name': 'sales', 'type': 'FLOAT'}
    ]
    
    load_to_bq_fb = GCSToBigQueryOperator(
        task_id='load_to_bq_fb',
        bucket='cf-data-mkt',
        source_objects=['marketing_campaing_fb.csv'],
        destination_project_dataset_table='cf_bronce_zone_1.marketing_campaing_fb',
        schema_fields=schema_reports,
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        gcp_conn_id='google-connection'
    )
    
    load_to_bq_gads = GCSToBigQueryOperator(
        task_id='load_to_bq_gads',
        bucket='cf-data-mkt',
        source_objects=['marketing_campaing_gads.csv'],
        destination_project_dataset_table='cf_bronce_zone_1.marketing_campaing_gads',
        schema_fields=schema_reports,
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        gcp_conn_id='google-connection'
    )
    
    load_to_bq_ytads = GCSToBigQueryOperator(
        task_id='load_to_bq_ytads',
        bucket='cf-data-mkt',
        source_objects=['marketing_campaing_ytads.csv'],
        destination_project_dataset_table='cf_bronce_zone_1.marketing_campaing_ytads',
        schema_fields=schema_reports,
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        gcp_conn_id='google-connection'
    )
    
    transform_query = '''
    CREATE OR REPLACE TABLE `cf_gold_zone_1.marketing_campaing` AS  
    SELECT *, "Facebook_ads" as source FROM `cf_bronce_zone_1.marketing_campaing_fb`
    UNION ALL
    SELECT *, "Google_ads" as source FROM `cf_bronce_zone_1.marketing_campaing_gads`
    UNION ALL
    SELECT *, "Youtube_ads" as source FROM `cf_bronce_zone_1.marketing_campaing_ytads`
    '''
    
    transform_data = BigQueryExecuteQueryOperator(
        task_id='transform_data',
        sql=transform_query,
        use_legacy_sql=False,
        gcp_conn_id='google-connection'
    )
    
    start >> [extract_data_fb, extract_data_gads, extract_data_ytads]
    extract_data_fb >> upload_to_gcs_fb >> load_to_bq_fb
    extract_data_gads >> upload_to_gcs_gads >> load_to_bq_gads
    extract_data_ytads >> upload_to_gcs_ytads >> load_to_bq_ytads
    [load_to_bq_fb, load_to_bq_gads, load_to_bq_ytads] >> transform_data >> end