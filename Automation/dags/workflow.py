from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from checkNewdata import checknewdata
from DataToLake import DataToLake_main
from DataToWarehouse import DataToWarehouse_main
    

with DAG(
    'ETL',
    default_args={
        'depends_on_past': False, #每一次執行的Task是否會依賴於上次執行的Task，如果是False的話，代表上次的Task如果執行失敗，這次的Task就不會繼續執行
        # 'email': ['airflow@example.com'], #如果Task執行失敗的話，要寄信給哪些人的email
        # 'email_on_failure': False, #如果Task執行失敗的話，是否寄信
        # 'email_on_retry': False, #如果Task重試的話，是否寄信
        'retries': 1, #最多重試的次數
        # 'retry_delay': timedelta(minutes=5), #每次重試中間的間隔
    },
    description='ETL process',
    schedule_interval= timedelta(days=7) , #'*/3 * * * *', #timedelta(days=7), None
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['af_etl_dag'],
) as dag:

# Task to insert data into MongoDB
    t0 = PythonOperator(
        task_id='CheckNewData',
        python_callable=checknewdata,
        provide_context=True,
        dag=dag,
    )

    t1 = PythonOperator(
        task_id='DataToLake',
        python_callable=DataToLake_main,
        provide_context=True,
        dag=dag,
    )

    t2 = PythonOperator(
        task_id='DataToWarehouse',
        python_callable=DataToWarehouse_main,
        provide_context=True,
        dag=dag,
    )
    t0 >> t1
    t1 >> t2