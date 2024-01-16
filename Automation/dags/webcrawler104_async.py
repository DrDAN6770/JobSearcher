import pandas as pd
import re, time, requests
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

from aiohttp import ClientSession, TCPConnector
import asyncio

class eJob_search104():
    current_date = datetime.now().date()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    search_url = 'https://www.104.com.tw/jobs/search/?'

    def __init__(self, filter_params, key_word, page = 15):
        self.filter_params = filter_params
        self.key_word = key_word
        self.page = page
           
    def search_total_jobs_link(self) -> list:
        url = requests.get(self.search_url, self.filter_params, headers=self.headers).url
        print(url)
        option = Options()
        option.add_argument(f"user-agent={self.headers['User-Agent']}")
        option.add_experimental_option('excludeSwitches', ['enable-automation']) # 開發者模式。可以避開某些防爬機制，有開有保佑
        # option.add_argument('--headless') # 無頭模式，開發完成之後再使用，可以完全背景執行，有機會變快
        # option.add_argument("--disable-gpu") # 禁用GPU加速，有些情況下需要設置這個參數
        driver = webdriver.Remote(
            command_executor='http://automation-chrome-1:4444/wd/hub',
            options=option
        )
        driver.get(url)

        element = driver.find_element(By.XPATH,'//*[@id="js-job-header"]/div[1]/label[1]/select/option[1]')
        total_page = int(re.sub(r'\D', '', element.text.split('/')[-1]))
        print(f'Total_page = {total_page}')
        # 滾頁面
        scroll_times = self.page if total_page >= 15 else total_page
        for _ in range(scroll_times):
            driver.execute_script('window.scrollTo(0,document.body.scrollHeight);')
            time.sleep(2)

        # 自動加載結束後要自行點選載入(15以後)
        # 使用CSS選擇器定位最後一個按鈕並點擊
        if total_page >= 15:
            k = 1
            while True:
                try:
                    button_element = WebDriverWait(driver, 4).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, '#js-job-content > div:last-child > button'))
                    )
                    print(f'手動載入第{15 + k}頁')
                    button_element.click()
                    k += 1
                    if k == 86 or k == total_page - 14 :
                        break
                except Exception as e:
                    print("發生未知錯誤：", e)
                    break

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        raw_Job_list = soup.find_all("a",class_="js-job-link")
        print(f'共{len(raw_Job_list)}筆資料')
        driver.quit()
        return raw_Job_list
    
    def filter_jobs_link(self, raw_Job_list : list) -> list:
        filter_job_list = [i for i in raw_Job_list if re.search(self.key_word, i['title'].lower())]
        print(f'過濾完有{len(filter_job_list)}筆')
        return filter_job_list
    
    async def getAPI_respone(self, url : str) -> dict:
        pattern = r'job/(.*?)\?jobsource'
        match = re.search(pattern, url)
        if match:
            apiurl = 'https://www.104.com.tw/job/ajax/content/' + match.group(1)
            header = {
                'Accept':'application/json, text/plain, */*',
                'Accept-Encoding':'gzip, deflate, br',
                'Accept-Language':'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                'Connection':'keep-alive',
                'Host':'www.104.com.tw',
                'Referer':url,
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            async with ClientSession(headers=header) as session:
                async with session.get(apiurl) as response:
                    # response.raise_for_status()
                    return await response.json()
        return {}

    async def get_Job_info_fromAPI(self, item : str) -> pd.DataFrame:
        url =  f"https:{item['href']}"
        alldata = await self.getAPI_respone(url)

        if not alldata:
            return None
        
        try:
            res = {}
            res['更新日期'] = [alldata['data']['header']['appearDate']]
            res['職缺名稱'] = [alldata['data']['header']['jobName']]
            res['公司名稱'] = [alldata['data']['header']['custName']]
            res['工作內容'] = [alldata['data']['jobDetail']['jobDescription']]
            res['職務類別'] = ['、'.join(job_category['description'] for job_category in alldata['data']['jobDetail']['jobCategory'])]
            res['工作待遇'] = [alldata['data']['jobDetail']['salary']]
            res['工作性質'] = [alldata['data']['jobDetail']['jobType']]
            res['上班地點'] = [alldata['data']['jobDetail']['addressRegion'] + alldata['data']['jobDetail']['addressDetail']]
            res['管理責任'] = [alldata['data']['jobDetail']['manageResp']]
            res['出差外派'] = [alldata['data']['jobDetail']['businessTrip']]
            res['上班時段'] = [alldata['data']['jobDetail']['workPeriod']]
            res['休假制度'] = [alldata['data']['jobDetail']['vacationPolicy']]
            res['可上班日'] = [alldata['data']['jobDetail']['startWorkingDay']]
            res['需求人數'] = [alldata['data']['jobDetail']['needEmp']]

            res['工作經歷'] = [alldata['data']['condition']['workExp']]
            res['學歷要求'] = [alldata['data']['condition']['edu']]

            major = '、'.join(major for major in alldata['data']['condition']['major'])
            res['科系要求'] = [major if major else '不拘']

            language = '、'.join(lang['language'] for lang in alldata['data']['condition']['language'])
            res['語文條件'] = [language if language else '不拘']

            specialty = '、'.join(specialty['description'] for specialty in alldata['data']['condition']['specialty'])
            res['擅長工具'] = [specialty if specialty else '不拘']

            skill = '、'.join(skill['description'] for skill in alldata['data']['condition']['skill'])
            res['工作技能'] = [skill if skill else '不拘']

            other = alldata['data']['condition']['other']
            res['其他要求'] = [other if other else '不拘']
            res['連結'] = [url]
            
            return pd.DataFrame(res)
        
        except Exception as e:
            print("發生錯誤", e)
            return None

    async def scrape(self, Job_list : list):
        tasks = []
        semaphore = asyncio.Semaphore(10)  # Limit concurrent requests to 10

        for item in Job_list:
            async with semaphore:
                task = asyncio.ensure_future(self.get_Job_info_fromAPI(item))
                tasks.append(task)

        return await asyncio.gather(*tasks)
    
    def main(self, Job_list: list, DF):
        batch_size = 30
        num_batches = (len(Job_list) + batch_size - 1) // batch_size
        for batch_idx in range(num_batches):
            start_idx = batch_idx * batch_size
            end_idx = min((batch_idx + 1) * batch_size, len(Job_list))
            Job_list_batch = Job_list[start_idx:end_idx]
            loop = asyncio.get_event_loop()
            results = loop.run_until_complete(self.scrape(Job_list_batch))
            # loop.close()

            for df in results:
                if df is not None:
                    DF = pd.concat([DF, df], ignore_index=True)

        DF.to_csv(f"output/JBLIST_{self.current_date}.csv", sep=',', index=False)
        return DF
    
def webcrawler_main():
    start_time = time.time()

    # 過濾關鍵字以外的職缺
    keywords_pattern = r'工程|資料|python|data|數據'

    # 搜尋關鍵字
    filter_params = {
        'ro' : 1, # 1 全職
        'keyword' : '資料工程',
        'area' : '6001002000,6001001000,6001006000,C6001008000',  # 6001001000 台北市 6001002000 新北 6001006000 新竹縣市 6001008000 台中市
        'isnew' : 3, # 0:本日 3:3天內 7:1週內 14 30
        'jobexp' : '1,3', # 工作經驗1年以下 + 1-3年
        'mode' : 'l', # 列表模式(比較多筆資料)
    }

    # 建立物件
    DF = pd.DataFrame()
    EJS = eJob_search104(filter_params, keywords_pattern)
    retry_count = 0
    while True:
        try:
            raw_Job_list = EJS.search_total_jobs_link()
            break
        except:
            retry_count += 1
            if retry_count == 3:
                break
            print(f'執行錯誤, retry {retry_count}')
    Job_list = EJS.filter_jobs_link(raw_Job_list)
    print("開始蒐集資料，請稍等....")
    result_df = EJS.main(Job_list, DF)
    print(f"{'=' * 30}蒐集資料結束{'=' * 30}")
    print(f"{'=' * 30}初步處理資料開始{'=' * 30}")

    # check missing data
    result_df[result_df.isnull().any(axis=1)]
    clean_df = result_df.dropna()
    clean_df = clean_df.drop_duplicates()

    # city
    clean_df['縣市'] = clean_df['上班地點'].apply(lambda x:x[:3])
    print(f"{'=' * 30}初步處理資料結束{'=' * 30}")

    current_date = datetime.now().date()
    if clean_df.isnull().sum().sum() == 0:
        clean_df.to_csv(f"output/JBLIST_{current_date}.csv", sep=',', index=False)
        print(f"{'=' * 30}程式執行完成{'=' * 30}")
        print(f"花費 {time.time() - start_time:.2f} 秒")
        return f"JBLIST_{current_date}.csv"
    else:
        print(f"{'=' * 30}NAN存在, 請檢察{'=' * 30}")
    
if __name__ == "__main__":
    webcrawler_main()