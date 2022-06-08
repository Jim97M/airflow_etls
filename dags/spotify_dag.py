from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from spotify_etl import run_spotify_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0,0,0,0,0),
    'email': ['wabuyajames@gmail.com'],
    'email_on_fail': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_on_delay': timedelta(minutes=1)

}


dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description = 'Spotify DAG',
    schedule_interval = timedelta(minutes=1)
)

def just_a_function():
    print("Show Tasks:")

def create_dag_alert(config):


 run_etl = PythonOperator(
    task_id = 'Whole_Spotify_ETL',
    python_callable = run_spotify_etl,
    dag=dag
)

run_etl
