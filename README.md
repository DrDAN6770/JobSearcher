# JobSearcher
![image](https://github.com/DrDAN6770/JobSearcher/assets/118630187/822dd064-62b9-4bfb-8389-6b915e8003e0)

# ERD
![image](https://github.com/DrDAN6770/JobSearcher/assets/118630187/51b39d52-495b-43f4-b238-6151d458fa04)

# Airflow
![image](https://github.com/DrDAN6770/JobSearcher/assets/118630187/b5418b66-6f1e-462d-8572-fa928055c588)

# API
![image](https://github.com/DrDAN6770/JobSearcher/assets/118630187/4292e15a-f761-4e67-b3a0-a30a781637b9)

# Result
**Implement pipeline, ETL, DB management and Automation, Solve the situation that originally required manually collecting data, re-analyzing, and filtering data**

1. **Data PipeLine**

   ![image](https://github.com/DrDAN6770/JobSearcher/assets/118630187/30725668-f2de-4772-9f2f-5bfcb32e5bf2)
   
   * `Web source Data` >> `ETL` >> `Target Data Storage` >> `Application`
   * This project focuses on the processes before `Application`
  

2. **Storage**
    * `MongoDB` for **personal practice**. If you don't use it, you can directly load the data into `MySQL`
    * Store data through **`MongoDB`** for `Data Lake` and **`MySQL`** for `Data Warehouse` both built by **`Docker`**
    * Simple **`Normalization`** processing and **`creating index`** increase query performance

3. **Container**
   
   All services built by **`Docker`**, including `MySQL`, `MongoDB`, `Crawler(selenium)`, `Airflow`, `FastAPI`

4. **Automation**

    * Use **`Airflow`** , and regularly collect data task because...
    * Famous, Good UI, Log interface
    * Generate an architecture diagram based on **`DAG`** to understand the entire process
   
5. **Crawler**

   Use **`Asynchronous operation`** instead of **`Synchronous operation`** to significantly speed up the execution speed (about **`95%`** reduction in operating time for the amount of data updated in a week, `4 hours to 10 mins`)

6. **API**
    * Using **`FastAPI`** get Data from Data warehouse to analysis (for **personal practice**)
    * Also can write sql code through connected DB to get data
    

## Motivation
* **104** is one of the most commonly used job search websites in Taiwan, but there are lots of job hunting information.
Is there a way to quickly search and filter the information?

* Or look at the current job market situation from an overall perspective
For example, salary distribution, what are the mainstream skills in the market, what industries are very hot, etc.

* Build a **Data pipeline** through **regular web crawler** search, **storage** and continuous updating

![image](https://github.com/DrDAN6770/JobSearcher/assets/118630187/3a2ffa50-6405-4183-a88c-73d1944d4ab6)

## Concept
1. Define the required parameters
    
    * keywords_pattern : Filter job titles that have already been searched
    * filter_params : search keywords
        
        * ro : 0(All), full-time(1)
        * keyword : Text when searching
        * area : area
        * isnew : Last updated date length
        * jobexp : Years of work experience required
        * mode : web displayed mode
        * order : Sort type
    ```
    keywords_pattern = r'工程|資料|python|data|數據'
    filter_params = {
        'ro' : 1,
        'keyword' : '資料工程',
        'area' : '6001002000,6001001000,6001006000,C6001008000',
        'isnew' : 14,
        'jobexp' : '1,3',
        'mode' : 'l',
        'order' : 16
    }
    ```

2. Create object and Execute
    ```
    DF = pd.DataFrame()
    EJS = eJob_search104(filter_params, keywords_pattern)
    while True:
        try:
            raw_Job_list = EJS.search_job()
            break
        except:
            print('執行錯誤, retry')
    Job_list = EJS.filter_job(raw_Job_list)
    result_df = EJS.main(Job_list, DF)
    ```

3. Save to csv file
4. Save to Data Lake
5. Save to Data Warehouse
6. Analysis Data

## Demo EDA
![image](https://github.com/DrDAN6770/JobSearcher/assets/118630187/aa453aaa-6f09-43fb-9899-97c108d58178)

![image](https://github.com/DrDAN6770/JobSearcher/assets/118630187/770a6ece-e02d-4b91-bdd6-f666022364a0)

![image](https://github.com/DrDAN6770/JobSearcher/assets/118630187/05fcbebb-79b0-450d-957e-4a8b7fea3268)
