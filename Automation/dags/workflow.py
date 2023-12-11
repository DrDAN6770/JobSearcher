from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from webcrawler104_async import webcrawler_main
from checkNewdata import checknewdata
from DataToLake import DataToLake_main
from DataToWarehouse import DataToWarehouse_main

def branch(**kwargs):
    res = kwargs['ti'].xcom_pull(task_ids='CheckNewData')

    if not res:
        print(f"{'=' * 50} DO 'DataCollection' task {'=' * 50}")
        return 'DataCollection'
    
    elif res[0] is True:
        print(f"{'=' * 50} Today already DONE {'=' * 50}")
        return

    else:
        print(f"{'=' * 50} DO 'DataToLake' task {'=' * 50}")
        return 'DataToLake'
    
    
        

def DataToLake_main_inDAG(**kwargs):
    ti = kwargs['ti']
    OldToDo = ti.xcom_pull(task_ids='CheckNewData')
    Newfile = ti.xcom_pull(task_ids='DataCollection')
    print(OldToDo, "!" * 10)
    print(Newfile, "!" * 10)

    if not OldToDo and not Newfile:
        print(f'{"==" * 30}No Data need to load!{"==" * 30}')
        return

    DataToLake_main(OldToDo, Newfile)


with DAG(
    'ETL',
    default_args={
        'depends_on_past': False, #每一次執行的Task是否會依賴於上次執行的Task，如果是False的話，代表上次的Task如果執行失敗，這次的Task就不會繼續執行
        # 'email': ['danny6770@gmail.com'], #如果Task執行失敗的話，要寄信給哪些人的email
        # 'email_on_failure': True, #如果Task執行失敗的話，是否寄信
        # 'email_on_retry': False, #如果Task重試的話，是否寄信
        'retries': 1, #最多重試的次數
        # 'retry_delay': timedelta(minutes=5), #每次重試中間的間隔
    },
    description='ETL process',
    schedule_interval= '0 0 * * 1,4' , #'*/3 * * * *', #timedelta(days=7), None
    start_date=datetime(2023, 12, 1),
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
        python_callable=DataToLake_main_inDAG,
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