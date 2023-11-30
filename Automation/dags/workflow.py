from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from webcrawler104_async import webcrawler_main
from checkNewdata import checknewdata
from DataToLake import DataToLake_main
from DataToWarehouse import DataToWarehouse_main

def branch(**kwargs):
    res = kwargs['ti'].xcom_pull(task_ids='CheckNewData')
    return 'DataToLake' if res else 'DataCollection'


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
    schedule_interval= timedelta(days=3) , #'*/3 * * * *', #timedelta(days=7), None
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['af_etl_dag'],
) as dag:
    t0 = PythonOperator(
        task_id='CheckNewData',
        python_callable=checknewdata,
        provide_context=True,
        dag=dag,
    )

    branch_task = BranchPythonOperator(
        task_id='Branch_condition',
        python_callable=branch,
        provide_context=True,
        dag=dag,
    )

    t1 = PythonOperator(
        task_id='DataCollection',
        python_callable=webcrawler_main,
        dag=dag,
    )  

    t2 = PythonOperator(
        task_id='DataToLake',
        python_callable=DataToLake_main,
        dag=dag,
        trigger_rule='none_failed_or_skipped'
    )

    t3 = PythonOperator(
        task_id='DataToWarehouse',
        python_callable=DataToWarehouse_main,
        dag=dag,
    )

    # logic
    # if t0 return true :then t0 > t2 > t3
    # if t0 return false: then t1 > t0 > t2 > t3
    t0 >> branch_task
    branch_task >> t1 >> t2 >> t3
    branch_task >> t2 >> t3