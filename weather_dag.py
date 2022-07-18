from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'tasks',
    'depends_on_past': True,
    'retries': 2,
    'retry_delay': timedelta(hours=3)
} 

with DAG(
       'tasks',
        default_args=default_args,
        schedule_interval='@daily',
        description='Push-Pull workflow',
        start_date=pendulum.datetime(2022, 7, 11, tz='UTC'),
        tags=['scrape', 'database'],
        catchup=False
    ) as dag:
    task_push = BashOperator(
        task_id='tasks',
        bash_command='python3 /home/james/airflow/dags/pull.py',
    )
    