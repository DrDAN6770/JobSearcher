import pandas as pd
import pymongo
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from checkNewdata import checknewdata

class dataToLake():
    def __init__(self, collectionname = None):
        load_dotenv()
        self.mongodb_user = os.getenv("mongodb_user")
        self.mongodb_password = os.getenv("mongodb_password")
        self.mongodb_port = os.getenv("mongodb_port")
        self.NoSQL_DB = 'JobDB'
        
        if collectionname:
            self.collectionname = collectionname
    
    def NoSQL_replace_data(self, *args):
        mongo_path = f"mongodb://{self.mongodb_user}:{self.mongodb_password}@mongodb:{self.mongodb_port}/"
        client = pymongo.MongoClient(mongo_path)
        db = client[self.NoSQL_DB]
        collection = db[self.collectionname]
        new_count, update_count = 0, 0
        for df in args:
            data = df.to_dict(orient="records")
            for idx, record in enumerate(data):
                try:
                    filter_query = {"連結": record["連結"]}
                    existing_record = collection.find_one(filter_query)
                    # 已存在就更新;不存在就插入
                    if existing_record is None:
                        new_count += 1
                        collection.insert_one(record)
                    else:
                        update_count += 1
                        collection.replace_one(filter_query, record)
                except Exception as e:
                    print(f'{idx},{e}')
                    continue
        print(f'更新{update_count}筆, 新增{new_count}筆')

def DataToLake_main(**kwargs):
    ti = kwargs['ti']
    path = ti.xcom_pull(task_ids='CheckNewData')
    if path:
        df = pd.read_csv(path)

    # check
    try:
        while df.isnull().sum().sum() != 0:
            number = df.isnull().sum().sum()
            print(f"NaN exist {number}")
            if number >= 10:
                print("manual handle")
                df[df.isnull().any(axis=1)]
                return
            else:
                df = df.dropna()
                print("Auto handle")
        
        Load = dataToLake('jobdata')
        Load.NoSQL_replace_data(df)
        print("Data to Lake Done!")
    except NameError:
        print("no Data need to load")
        return
    except Exception as e:
        print(e)
        return

if __name__ == "__main__":    
    DataToLake_main()