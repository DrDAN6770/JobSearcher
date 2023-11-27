import pandas as pd
import pymongo
from datetime import datetime

class dataToLake():
    NoSQL_DB = 'JobDB'
    def __init__(self, collectionname = None):
        if collectionname:
            self.collectionname = collectionname
    
    def NoSQL_replace_data(self, *args):
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client[self.NoSQL_DB]
        collection = db[self.collectionname]
        new_count, update_count = 0, 0
        current_date = datetime.now().date()
        for df in args:
            df['搜集資料日期'] = current_date.strftime('%Y-%m-%d')
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

def DataToLake_main():
    # Load csv (通常一次一個, 每次search完匯入SQL)
    current_date = datetime.now().date()
    file_name = f"JBLIST_{current_date}.csv"
    df = pd.read_csv(f'../output/{file_name}')

    # check
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

if __name__ == "__main__":    
    DataToLake_main()