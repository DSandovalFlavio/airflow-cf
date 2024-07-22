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
    'start_date': datetime(2024, 3, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# create a DAG object
with DAG(
    'practica_MSPM_dag',
    default_args=default_args,
    description='Practica MSPM',
    schedule_interval='@hourly',
    tags=['Practica', 'Codigo Facilito', 'Flavio Sandoval']
) as dag:
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    # lista de sensores que estuvieran en una planta de manufactura
    list_sensor = ['seccion_a_temperatura', 'seccion_a_presion', 'seccion_a_vibracion', 
                'seccion_b_temperatura', 'seccion_b_presion', 'seccion_b_vibracion', 
                'seccion_c_temperatura', 'seccion_c_presion', 'seccion_c_vibracion']
    
    def extract_data_sensor(sensor):
        print(sensor)
    
    list_tasks_extract_data_sensor = []
    for sensor in list_sensor:
        task = PythonOperator(
            task_id=f'extract_data_{sensor}',
            python_callable=extract_data_sensor,
            op_args=[sensor]
        )
        list_tasks_extract_data_sensor.append(task)
    
    ingest_db = DummyOperator(task_id='ingest_db')
    predict_analytics = DummyOperator(task_id='predict_analytics')
    alert_supervisor = DummyOperator(task_id='alert_supervisor')
    alert_maintenance = DummyOperator(task_id='alert_maintenance')
    update_dashboard_production = DummyOperator(task_id='update_dashboard_production')
    update_dashboard_maintenance = DummyOperator(task_id='update_dashboard_maintenance')
    
    start >> list_tasks_extract_data_sensor >> ingest_db >> predict_analytics 
    predict_analytics >> [alert_supervisor, 
                          alert_maintenance, 
                          update_dashboard_production, 
                          update_dashboard_maintenance] >> end
    
