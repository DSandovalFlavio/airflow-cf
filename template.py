from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

default_args = {
    'owner': 'Codigo Facilito Team',
    'start_date': datetime(2024,5,1),
    'depends_on_past': False,
}

with DAG('mostrar_kwargs', 
        default_args=default_args, 
        schedule_interval="@daily",
        tags=['Ingenieria']
        ) as dag:

    def mostrar_kwargs(**kwargs):
        print("Kwargs:")
        for clave, valor in kwargs.items():
            print(f"  Clave: {clave}, Valor: {valor}, typo: {type(valor)}")

    mostrar_kwargs_tarea = PythonOperator(
        task_id='mostrar_kwargs',
        python_callable=mostrar_kwargs
    )

    mostrar_kwargs_tarea