import pandas as pd
from fastapi import FastAPI
from sqlalchemy import create_engine, engine
import os
from dotenv import load_dotenv


def get_mysql_conn() -> engine.base.Connection:
    load_dotenv()
    mysql_user = os.getenv("mysqldb_user")
    mysql_password = os.getenv("mysqldb_password")
    mysql_port = os.getenv("mysqldb_port")

    address = f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@mysqldb:{mysql_port}/mydb"
    engine = create_engine(address)
    connect = engine.connect()
    return connect


app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "Find Job Data?"}


@app.get("/Table & limit")
def get_one_table_sqlData(table_name : str, n : str = None):   
    sql = f"select * from {table_name}" if not n else f"select * from {table_name} limit {n}"

    mysql_conn = get_mysql_conn()
    df = pd.read_sql(sql, con=mysql_conn)
    data_dict = df.to_dict("records")
    return {"data": data_dict}

@app.get("/RDBMS by Date")
def get_RDBM_sqlData(date : str):
    mysql_conn = get_mysql_conn()
    query = f'''
        SELECT
            *
        FROM
            JobsInfo
        WHERE
            更新日期 >= {date}
        
    '''
    df = pd.read_sql(query, mysql_conn)
    data_dict = df.to_dict("records")
    return {"data": data_dict}

@app.get("/detail jobs data with limit & specific title")
def get_RDBM_sqlData(limit : str, title_keyword : str = None):
    mysql_conn = get_mysql_conn()
    base_query = f'''
        SELECT
            t0.更新日期
            ,t0.職缺名稱
            ,t0.公司名稱
            ,t1.category AS "職務類別"
            ,t0.工作待遇
            ,t2.city AS "縣市"
            ,t0.上班地點
            ,t3.management AS "管理責任"
            ,t4.type AS "出差外派"
            ,t0.上班時段
            ,t5.yearexp AS "工作經歷"
            ,t0.語文條件
            ,t0.擅長工具
            ,t0.工作技能
            ,t0.連結
        FROM
            JobsInfo AS t0
        JOIN
            JobCategory AS t1 ON t1.id = t0.職務類別
        JOIN
            City AS t2 ON t2.id = t0.縣市
        JOIN
            ManagementResponsibility AS t3 ON t3.id = t0.管理責任
        JOIN
            Business_trip AS t4 ON t4.id = t0.出差外派
        JOIN
            WorkingEXP AS t5 ON t5.id = t0.工作經歷
        
    '''
    title_keyword_query = f'''
        WHERE t0.職缺名稱 Like "%{title_keyword}%"

    '''
    end_query = f'''
        ORDER BY
            更新日期 DESC
        LIMIT
            {limit}
    '''

    query = base_query + title_keyword_query + end_query if title_keyword else base_query + end_query
    df = pd.read_sql(query, mysql_conn)
    data_dict = df.to_dict("records")
    return {"data": data_dict}

@app.get("/specific job_link")
def get_RDBM_sqlData(job_link : str):
    mysql_conn = get_mysql_conn()
    query = f'''
        SELECT
            *
        FROM
            JobsInfo
        WHERE
            連結 = '{job_link}'
    '''
    df = pd.read_sql(query, mysql_conn)
    data_dict = df.to_dict("records")
    return {"data": data_dict}