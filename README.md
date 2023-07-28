# JobSearcher
There are **2** crawler methods
1. **Synchronous processing**
2. **Asynchronous processing**

## Motivation
* **104** is one of the most commonly used job search websites in Taiwan, but there are lots of job hunting information.
Is there a way to quickly search and filter the information?

* Or look at the current job market situation from an overall perspective
For example, salary distribution, what are the mainstream skills in the market, what industries are very hot, etc.

* Build a **data pipeline** through **regular web crawler** search and storage and continuous updating

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
    ![image](https://github.com/DrDAN6770/JobSearcher/assets/118630187/6e620f1b-5837-4545-af0f-6b8cae96690d)

3. Save to csv file:
    ```
    current_date = datetime.now().date()
    clean_df.to_csv(f"JBLIST_{current_date}.csv", sep='|', index=False)
    ```
## Demo EDA
![image](https://github.com/DrDAN6770/JobSearcher/assets/118630187/d68d5f03-5337-4885-8424-7ab91b792e1a)

![image](https://github.com/DrDAN6770/JobSearcher/assets/118630187/05fcbebb-79b0-450d-957e-4a8b7fea3268)

![image](https://github.com/DrDAN6770/JobSearcher/assets/118630187/0edc96a1-0a48-43f9-81f6-a3a281b92fff)

![image](https://github.com/DrDAN6770/JobSearcher/assets/118630187/5c3958ba-58f1-45d3-9a7f-fcc9f3fb4171)


## Environments and Moduels (2023/07)
1. Python 3.11.2
2. request 2.31.0
3. pandas 2.0.1
4. selenium 4.10.0
5. beautifulsoup 4.12.2
6. aiohttp 3.8.5
7. re, time, random, datetime, asyncio

