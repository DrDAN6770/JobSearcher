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
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'
    }
    search_url = 'https://www.104.com.tw/jobs/search/?'
    def __init__(self, filter_params, key_word, page = 15):
        self.filter_params = filter_params
        self.key_word = key_word
        self.page = page
           
    def search_job(self):
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
    
    def filter_job(self, raw_Job_list:list):
        filter_job_list = [i for i in raw_Job_list if re.search(self.key_word, i['title'].lower())]
        print(f'過濾完有{len(filter_job_list)}筆')
        return filter_job_list
    
    def update_date(self, soup) -> str:
        update_date = soup.find("div", class_="job-header__title")
        return update_date.find('span').text.strip().replace('更新','') if update_date else '無'
    
    def company(self, soup) -> str:
        name = soup.find("div", class_="mt-3")
        return name.select_one('div > a').text.strip() if name else '無'
    
    def jd_info(self, soup) -> dict:
        result = {}
        JD = soup.find('div', class_='job-description-table row')
        if JD:
            jd_items = JD.find_all('div', recursive=False)
            if jd_items:
                    try:
                        job_content = jd_items[0].find('p').text if jd_items[0].find('p').text else '無'
                    except:
                        job_content = '無'

                    try:
                        job_category = ', '.join(i.text for i in jd_items[1].find_all('u')) if jd_items[1].find_all('u') else '無'
                    except:
                        job_category = '無'
                    
                    try:
                        salary = jd_items[2].find_all('div', recursive=False)[-1].text.strip() if jd_items[2].find_all('div', recursive=False)[-1].text.strip() else '無'
                    except:
                        salary = '無'
                    
                    try:
                        job_type = jd_items[3].find_all('div')[-1].text.strip() if jd_items[3].find_all('div')[-1].text.strip() else '無'
                    except:
                        job_type = '無'

                    try:
                        workingplace = jd_items[4].find_all('div')[-1].text.strip() if jd_items[4].find_all('div')[-1].text.strip() != '代企業徵才' else None
                    except:
                        workingplace = '無'

                    try:
                        management_responsibility = jd_items[6].find_all('div')[-1].text.strip() if jd_items[6].find_all('div')[-1].text.strip() else '無'
                    except:
                        management_responsibility = '無'
                    
                    try:
                        business_trip = jd_items[7].find_all('div')[-1].text.strip() if jd_items[7].find_all('div')[-1].text.strip() else '無'
                    except:
                        business_trip = '無'

                    try:
                        working_duration = jd_items[8].find_all('div')[-1].text.strip() if jd_items[8].find_all('div')[-1].text.strip() else '無'
                    except:
                        working_duration = '無'
                    
                    try:
                        Holiday_System = jd_items[9].find_all('div')[-1].text.strip() if jd_items[9].find_all('div')[-1].text.strip() else '無'
                    except:
                        Holiday_System = '無'
                    
                    try:
                        Working_date = jd_items[10].find_all('div')[-1].text.strip() if jd_items[10].find_all('div')[-1].text.strip() else '無'
                    except:
                        Working_date = '無'
                    
                    try:
                        ppl_required = jd_items[11].find_all('div')[-1].text.strip() if jd_items[11].find_all('div')[-1].text.strip() else '無'
                    except:
                        ppl_required = '無'


                    result = {
                                '工作內容': job_content,
                                '職務類別': job_category,
                                '工作待遇': salary,
                                '工作性質': job_type,
                                '上班地點': workingplace,
                                '管理責任': management_responsibility,
                                '出差外派': business_trip,
                                '上班時段': working_duration,
                                '休假制度': Holiday_System,
                                '可上班日': Working_date,
                                '需求人數': ppl_required
                            }
        return result
    
    def jr_info(self, soup) -> dict:
        result = {}
        JR = soup.find('div', class_= 'job-requirement-table row')
        JRO = soup.find('div', class_= 'job-requirement col opened')
        if JR:
            jr_items = JR.find_all('div', recursive=False)
            if jr_items:
                try:
                    work_exp = jr_items[0].find_all('div')[-1].text.strip() if jr_items[0].find_all('div')[-1].text.strip() else '無'
                except:
                    work_exp = '無'
                
                try:
                    academic_require = jr_items[1].find_all('div')[-1].text.strip() if jr_items[1].find_all('div')[-1].text.strip() else '無'
                except:
                    academic_require = '無'
                
                try:
                    department_require = jr_items[2].find_all('div')[-1].text.strip() if jr_items[2].find_all('div')[-1].text.strip() else '無'
                except:
                    department_require = '無'

                try:
                    language = jr_items[3].find('p').text.strip() if jr_items[3].find('p').text.strip() else '無'
                except:
                    language = '無'

                try:
                    tool = ', '.join(i.text for i in jr_items[4].find_all('u')) if jr_items[4].find_all('u') else '無'
                except:
                    tool = '無'
                
                try:
                    working_ability = jr_items[5].find_all('div')[-1].text.strip() if jr_items[5].find_all('div')[-1].text.strip() else '無'
                except:
                    working_ability = '無'
   
        try:
            others = JRO.find_all('div')[-1].text.strip() if JRO.find_all('div')[-1].text.strip() else '無'
        except:
            others = '無'


        if JR and JRO:
            result = {
                '工作經歷' : work_exp,
                '學歷要求' : academic_require,
                '科系要求' : department_require,
                '語文條件' : language,
                '擅長工具' : tool,
                '工作技能' : working_ability,
                '其他要求' : others
            }
        return result
    
    async def fetch(self, session, url):
        async with session.get(url, headers = {'User-Agent':'GoogleBot'}) as response:
            return await response.text()

    async def get_job_info(self, item):
        try:
            title = item['title']
            Job_link = f"https:{item['href']}"
            connector = TCPConnector(limit=10)
            async with ClientSession(connector=connector) as session:
                html = await self.fetch(session, Job_link)
                soup = BeautifulSoup(html, 'lxml')

            Data = {
                '更新日期': [self.update_date(soup)],
                '職缺名稱': [title],
                '公司名稱': [self.company(soup)],
                '連結': [Job_link]
            }
            Data.update(self.jd_info(soup))
            Data.update(self.jr_info(soup))
            df = pd.DataFrame(Data, columns=['更新日期', '職缺名稱', '公司名稱', '工作內容', '職務類別', '工作待遇',
                                            '工作性質', '上班地點', '管理責任', '出差外派', '上班時段', '休假制度',
                                            '可上班日', '需求人數', '工作經歷', '學歷要求', '科系要求', '語文條件',
                                            '擅長工具', '工作技能', '其他要求', '連結'])
            return df
        except Exception as e:
            print("發生錯誤", e)
            return None

    async def scrape(self, Job_list):
        tasks = []
        semaphore = asyncio.Semaphore(10)  # Limit concurrent requests to 10

        for item in Job_list:
            async with semaphore:
                task = asyncio.ensure_future(self.get_job_info(item))
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
            raw_Job_list = EJS.search_job()
            break
        except:
            retry_count += 1
            if retry_count == 3:
                break
            print(f'執行錯誤, retry {retry_count}')
    Job_list = EJS.filter_job(raw_Job_list)
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