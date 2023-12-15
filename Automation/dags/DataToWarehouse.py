from datetime import datetime
import pandas as pd
import pymongo
import os, time
from dotenv import load_dotenv
from sqlalchemy import create_engine, exc, update, insert, MetaData, Table

class dataToWarehouse():
    def __init__(self, tablename = None):
        load_dotenv()
        self.mysql_user = os.getenv("mysqldb_user")
        self.mysql_password = os.getenv("mysqldb_password")
        self.mysql_port = os.getenv("mysqldb_port")

        self.mongodb_user = os.getenv("mongodb_user")
        self.mongodb_password = os.getenv("mongodb_password")
        self.mongodb_port = os.getenv("mongodb_port")

        if tablename:
            self.tablename = tablename
            self.create_fact_schema()
        

    def create_fact_schema(self):
        db_url = f"mysql+mysqlconnector://{self.mysql_user}:{self.mysql_password}@mysqldb:{self.mysql_port}/mydb"
        engine = create_engine(db_url)
        sql_statement = """
            CREATE TABLE IF NOT EXISTS JobsInfo  (
                `更新日期` DATE,
                `職缺名稱` VARCHAR(100),
                `公司名稱` VARCHAR(100),
                `工作內容` TEXT,
                `職務類別` INT,
                `工作待遇` VARCHAR(100),
                `工作性質` INT,
                `縣市` INT,
                `上班地點` VARCHAR(100),
                `管理責任` INT,
                `出差外派` INT,
                `上班時段` VARCHAR(100),
                `休假制度` INT,
                `可上班日` INT,
                `需求人數` VARCHAR(50),
                `工作經歷` INT,
                `學歷要求` INT,
                `科系要求` VARCHAR(50),
                `語文條件` VARCHAR(50),
                `擅長工具` VARCHAR(500),
                `工作技能` VARCHAR(500),
                `其他要求` VARCHAR(500),
                `連結` TEXT,
                PRIMARY KEY (`連結`(255)),
                FOREIGN KEY (`職務類別`) REFERENCES JobCategory(id),
                FOREIGN KEY (`工作性質`) REFERENCES JobType(id),
                FOREIGN KEY (`縣市`) REFERENCES City(id),
                FOREIGN KEY (`管理責任`) REFERENCES ManagementResponsibility(id),
                FOREIGN KEY (`出差外派`) REFERENCES Business_trip(id),
                FOREIGN KEY (`休假制度`) REFERENCES HolidaySystem(id),
                FOREIGN KEY (`可上班日`) REFERENCES AvailableStartdate(id),
                FOREIGN KEY (`工作經歷`) REFERENCES WorkingEXP(id),
                FOREIGN KEY (`學歷要求`) REFERENCES Degree(id)
            );
        """
        try:
            with engine.connect() as connection:
                connection.execute(sql_statement)
            print("Table created successfully.")
        except exc.SQLAlchemyError as e:
            print(f"Error creating table: {e}")

    def LoadFromLake(self) -> pd.DataFrame:
        client = pymongo.MongoClient(f'mongodb://{self.mongodb_user}:{self.mongodb_password}@mongodb:{self.mongodb_port}/')
        db = client["JobDB"]
        collection = db["jobdata"]
        latest_date_record = collection.find_one(
            filter={},
            sort=[("搜集資料日期", -1)],  # 按照 "搜集資料日期" 字段降序排序
            projection={"_id": 0, "搜集資料日期": 1}  # 只返回 "搜集資料日期" 字段，不返回 "_id" 字段
        )
        print(f'上次更新日期 : {latest_date_record["搜集資料日期"]}')
        data_list = list(collection.find({"搜集資料日期": {"$gte": latest_date_record["搜集資料日期"]}}))
        if data_list:
            df = pd.DataFrame(data_list)
            df = df.drop(["_id", "搜集資料日期"], axis = 1)
            return df
        return pd.DataFrame()

    def Dateformat(self, df : pd.DataFrame) -> pd.DataFrame:
        current_year = datetime.now().year
        current_date = datetime.now().date()
        df['更新日期'] = f'{current_year}/' + df['更新日期']
        df['更新日期'] = pd.to_datetime(df['更新日期'], errors='coerce')
        df['更新日期'] = df['更新日期'].apply(lambda x: x.replace(year=current_year - 1) if x.date() > current_date else x)
        return df

        
    def TranslateData(self, df : pd.DataFrame, x : str, DimensionTable : str, col_name : str) -> pd.DataFrame:
        db_url = f"mysql+mysqlconnector://{self.mysql_user}:{self.mysql_password}@mysqldb:{self.mysql_port}/mydb"
        engine = create_engine(db_url)

        query = f"SELECT * FROM {DimensionTable}"
        dimension = pd.read_sql(query, engine)
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
        # 2. Date改格式
        # 3. 出差外派
        # 4. 學歷要求
        # 5. 職務類別
        # 6. 科系要求
        # 7. Transform

        # 新的列顺序
        new_columns_order = ['更新日期', '職缺名稱', '公司名稱', '工作內容', '職務類別',
                            '工作待遇', '工作性質', '縣市', '上班地點', '管理責任', '出差外派',
                            '上班時段', '休假制度', '可上班日', '需求人數', '工作經歷',
                            '學歷要求', '科系要求', '語文條件', '擅長工具', '工作技能',
                            '其他要求', '連結']

        df = self.LoadFromLake()
        if not df.empty:
            df = self.Dateformat(df)
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
        db_url = f"mysql+mysqlconnector://{self.mysql_user}:{self.mysql_password}@mysqldb:{self.mysql_port}/mydb"
        engine = create_engine(db_url)
        metadata = MetaData()
        jbinfo = Table(self.tablename, metadata, autoload_with=engine)
        start_time = time.time()

        # slow method 500筆資料 插入5秒更新4秒
        # for i in range(len(df)):
        #     try:
        #         df.iloc[i:i+1].to_sql(self.tablename, con = engine, index=False, if_exists='append')
        #         new_count += 1
        #     except exc.IntegrityError:
        #         row = df.iloc[i].to_dict()
        #         link = row.pop('連結')
        #         stmt = (
        #             update(jbinfo)
        #             .where(jbinfo.c.連結 == link)
        #             .values(row)
        #         )       
        #         with engine.connect() as connection:
        #             connection.execute(stmt)
        #         updates_count += 1

        # fast method 500筆資料 插入2秒更新1.3秒
        for i, row in df.iterrows():
            try:
                data = row.to_dict()
                stmt = (
                    insert(jbinfo)
                    .values(data)
                )
                with engine.connect() as connection:
                    connection.execute(stmt)
                new_count += 1
            except exc.IntegrityError:
                row = row.to_dict()
                link = row.pop('連結')
                stmt = (
                    update(jbinfo)
                    .where(jbinfo.c.連結 == link)
                    .values(row)
                )       
                with engine.connect() as connection:
                    connection.execute(stmt)
                updates_count += 1
            except Exception as e:
                print('=' * 100)
                print(i, row['連結'])
                print(e)
                print('=' * 100)
            
        print(f"ETL Done!\nTotal {len(df)}, update {updates_count}, add {new_count}")
        end_time = time.time()
        print(f"執行時間 : {end_time - start_time:.2f} 秒")
        return 0

def DataToWarehouse_main():
    ETL = dataToWarehouse('JobsInfo')
    df = ETL.process()
    if not df.empty:
        null_count = df.isnull().sum().sum()
        if null_count == 0:
            ETL.Load(df)
        elif null_count < 10:
            df = df.dropna()
            print(f"{'=' *50}There're some null, but less, Auto handle{'=' *50}")
            ETL.Load(df)
        else:
            print(f'{"=" * 50}Something wrong, please check!!{"=" * 50}')
            print(df.isnull().sum().sum())
    else:
        print(f"{'=' *50}No Data need to load!{'=' *50}")
        
if __name__ == "__main__":
    DataToWarehouse_main()