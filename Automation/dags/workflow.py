from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from websearch104 import execute

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 8)
}

dag = DAG('jobsearch',
          default_args=default_args,
          schedule_interval = '0 0 * * 4' # hour : mon : Month : week : day >> every thursday
        #   schedule_interval = timedelta(days=7)
)

task = PythonOperator(
    task_id='104search',
    python_callable=execute,
    dag=dag
)