import pandas as pd
import pymongo
import sqlite3
from datetime import datetime


class dataToWarehouse():
    SQL_DB = 'Jobsearch_database.db'
    columns_num = 23
    columns_name = '更新日期,職缺名稱,公司名稱,工作內容,職務類別,\
                    工作待遇,工作性質,縣市,上班地點,管理責任,出差外派,\
                    上班時段,休假制度,可上班日,需求人數,工作經歷,\
                    學歷要求,科系要求,語文條件,擅長工具,工作技能,\
                    其他要求,連結'
    def __init__(self, tablename = None):
        if tablename:
            self.tablename = tablename
            self.create_fact_schema()
            # crate index
            conn = sqlite3.connect(self.SQL_DB)
            crate_index_set = f"CREATE INDEX IF NOT EXISTS idx_city_exp ON {self.tablename} (縣市, 工作經歷)"
            conn.execute(crate_index_set)
            conn.commit()
            conn.close()

    def create_fact_schema(self):
        conn = sqlite3.connect(self.SQL_DB)
        create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {self.tablename}  (
            '更新日期' Date,
            '職缺名稱' VARCHAR(50),
            '公司名稱' VARCHAR(50),
            '工作內容' TEXT,
            '職務類別' INT REFERENCES JobCategory(id),
            '工作待遇' VARCHAR(50),
            '工作性質' INT REFERENCES JobType(id),
            '縣市' INT REFERENCES City(id),
            '上班地點' VARCHAR(50),
            '管理責任' INT REFERENCES ManagementResponsibility(id),
            '出差外派' INT REFERENCES Business_trip(id),
            '上班時段' VARCHAR(50),
            '休假制度' INT REFERENCES HolidaySystem(id),
            '可上班日' INT REFERENCES AvailableStartdate(id),
            '需求人數' VARCHAR(50),
            '工作經歷' INT REFERENCES WorkingEXP(id),
            '學歷要求' INT REFERENCES Degree(id),
            '科系要求' VARCHAR(50),
            '語文條件' VARCHAR(50),
            '擅長工具' VARCHAR(50),
            '工作技能' VARCHAR(50),
            '其他要求' VARCHAR(50),
            '連結' TEXT PRIMARY KEY
        )
        '''
        conn.execute(create_table_query)
        conn.commit()
        conn.close()

    def LoadFromLake(self) -> pd.DataFrame:
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["JobDB"]
        collection = db["jobdata"]
        latest_date_record = collection.find_one(
            filter={},
            sort=[("搜集資料日期", -1)],  # 按照 "搜集資料日期" 字段降序排序
            projection={"_id": 0, "搜集資料日期": 1}  # 只返回 "搜集資料日期" 字段，不返回 "_id" 字段
        )
        print(latest_date_record)
        data_list = list(collection.find({"搜集資料日期": {"$gte": latest_date_record["搜集資料日期"]}}))
        if data_list:
            df = pd.DataFrame(data_list)
            df = df.drop(["_id", "搜集資料日期"], axis = 1)
            return df
        return pd.DataFrame()
    
    def TranslateData(self, df : pd.DataFrame, x : str, DimensionTable : str, col_name : str) -> pd.DataFrame:
        conn = sqlite3.connect(self.SQL_DB)
        query = f"SELECT * FROM {DimensionTable}"
        dimension = pd.read_sql(query, conn)
        # swap key, value
        dimension = dimension.set_index(col_name)['id'].to_dict()
        df[x] = df[x].map(dimension)
        return df
    
    def translation_businesstrip(self, df : pd.DataFrame) -> pd.DataFrame:
        keyword = ['無需出差外派', '一年累積時間未定', '一年累積時間約一個月以下', '一年累積時間約三個月以下', '一年累積時間約六個月以下', '一年累積時間約七個月以上']
        map_str = ['無', '1年累積時間未定', '1年累積時間約1個月以下', '1年累積時間約3個月以下', '1年累積時間約6個月以下', '1年累積時間約7個月以上']
        for k, v in zip(keyword, map_str):
            df.loc[df['出差外派'].str.contains(k), '出差外派'] = v
        return df
    
    def translation_degree(self, df : pd.DataFrame) -> pd.DataFrame:
        keyword = ['高中|專科', '大學', '碩士', '博士']
        map_str = ['高中以上', '學士以上', '碩士以上', '博士以上']
        for k, v in zip(keyword, map_str):
            df.loc[df['學歷要求'].str.contains(k), '學歷要求'] = v
        return df
    
    def translation_category(self, df : pd.DataFrame) -> pd.DataFrame:
        keyword = ['資料工程師|數據工程師', '資料分析師|數據分析師', '資料科學', '資料庫', '演算法', '軟體工程師', 'AI', '系統', '網路|Internet', '助理']
        map_str = ['資料工程師', '資料分析師', '資料科學家', '資料庫管理人員', '演算法工程師', '軟體工程師', 'AI工程師', '系統工程師', '網路工程師', '助理工程師']
        for k, v in zip(keyword, map_str):
            df.loc[df['職務類別'].str.contains(k), '職務類別'] = v
        # others
        df.loc[~df['職務類別'].isin(map_str), '職務類別'] = '其他'
        return df

    def translation_department(self, df : pd.DataFrame) -> pd.DataFrame:
        keyword = ['資訊管理|資訊工程', '電機電子', '數學|統計', '不拘']
        map_str = ['資訊類別', '電機', '數學統計', '不拘']
        for k, v in zip(keyword, map_str):
            df.loc[df['科系要求'].str.contains(k), '科系要求'] = v
        # others
        df.loc[~df['科系要求'].isin(map_str), '科系要求'] = '其他'
        return df
    
    def process(self) -> pd.DataFrame:
        # 1. Extract
        # 2. 出差外派
        # 3. 學歷要求
        # 4. 職務類別
        # 5. 科系要求
        # 6. Transform

        # 新的列顺序
        new_columns_order = ['更新日期', '職缺名稱', '公司名稱', '工作內容', '職務類別',
                            '工作待遇', '工作性質', '縣市', '上班地點', '管理責任', '出差外派',
                            '上班時段', '休假制度', '可上班日', '需求人數', '工作經歷',
                            '學歷要求', '科系要求', '語文條件', '擅長工具', '工作技能',
                            '其他要求', '連結']

        df = self.LoadFromLake()

        if not df.empty:
            df = df[new_columns_order]
            df = self.translation_businesstrip(df)
            df = self.translation_degree(df)
            df = self.translation_category(df)
            df = self.translation_department(df)

            keys = [('可上班日', 'AvailableStartdate', 'AvailableType'), ('出差外派', 'Business_trip', 'type'),
                    ('縣市', 'City', 'city'), ('學歷要求', 'Degree', 'degree'), ('職務類別', 'JobCategory', 'category'),
                    ('休假制度', 'HolidaySystem', 'Holiday_type'), ('工作性質', 'JobType', 'type'), ('科系要求', 'Department', 'department'),
                    ('管理責任', 'ManagementResponsibility', 'management'), ('工作經歷', 'WorkingEXP', 'yearexp')]

            for X, DimensionTable, col_name in keys:
                df = self.TranslateData(df, X, DimensionTable, col_name)

            return df
        return pd.DataFrame()
    
    def Load(self, df):
        new_count = 0
        updates_count = 0
        conn = sqlite3.connect(self.SQL_DB)
        cursor = conn.cursor()
        for _, row in df.iterrows():
            try:
                insert_query = f"INSERT INTO {self.tablename} ({', '.join(row.index)}) VALUES ({', '.join(['?'] * len(row))})"
                cursor.execute(insert_query, row.tolist())
                new_count += 1
            # PK衝突 >> 重複PK >> 更新資料
            except sqlite3.IntegrityError:
                update_query = f"UPDATE {self.tablename} SET " + ", ".join([f"{col} = ?" for col in row.index if col != '連結']) + " WHERE 連結 = ?"
                lst = list(row.values)
                lst.pop()
                values = lst + [row['連結']]
                cursor.execute(update_query, values)
                updates_count += 1

        conn.commit()
        conn.close()

        print(f"ETL Done!\nTotal {len(df)}, update {updates_count}, add {new_count}")
        return 0

def DataToWarehouse_main() -> None:
    ETL = dataToWarehouse('JobsInfo')
    df = ETL.process()
    if not df.empty:
        if df.isnull().sum().sum() == 0:
            ETL.Load(df)
        else:
            print("Something wrong, please check!")
    else:
        print(f"{'='*30}No new data{'='*30}")
        
if __name__ == "__main__":
    DataToWarehouse_main()