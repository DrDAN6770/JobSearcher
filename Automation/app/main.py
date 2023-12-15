import pandas as pd
from fastapi import FastAPI
from sqlalchemy import create_engine, engine
import os
from dotenv import load_dotenv


def get_mysql_financialdata_conn() -> engine.base.Connection:
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


@app.get("/Table")
def get_one_table_sqlData(table_name : str):
    sql = f"select * from {table_name}"
    
    mysql_conn = get_mysql_financialdata_conn()
    df = pd.read_sql(sql, con=mysql_conn)
    data_dict = df.to_dict("records")
    return {"data": data_dict}

@app.get("/RDBMS")
def get_RDBM_sqlData(join_table_name : str, join_column : str, Dimension_column : str):
    mysql_conn = get_mysql_financialdata_conn()
    query = f'''
        SELECT
            t2.{Dimension_column} AS {join_column}
        FROM
            JobsInfo AS t1
        JOIN
            {join_table_name} AS t2
        ON
            t1.{join_column} = t2.id
        
    '''
    df = pd.read_sql(query, mysql_conn)
    data_dict = df.to_dict("records")
    return {"data": data_dict}