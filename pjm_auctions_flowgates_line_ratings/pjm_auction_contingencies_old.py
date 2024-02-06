import logging
import requests
from bs4 import BeautifulSoup
from lxml import html
from datetime import datetime
import re
import pandas as pd
import numpy as np
from bu_snowflake import get_connection
import bu_alerts
import os
import sys
import time
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
# from selenium.webdriver.common.action_chains import ActionChains
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support import expected_conditions
# from selenium.common.exceptions import NoSuchElementException
# from selenium.common.exceptions import StaleElementReferenceException

sys.path.append(r'\\biourja.local\biourja\Groups\Gas & Pwr\Pwr\FTR\Virtuals\Michael\FTR\sftp_to_sf')
from tosnowflake import load_df_to_sf

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)


job_id=np.random.randint(1000000,9999999)
# receiver_email = 'indiapowerit@biourja.com, DAPower@biourja.com'
receiver_email = 'radha.waswani@biourja.com'
# receiver_email = 'imam.khan@biourja.com, radha.waswani@biourja.com'
# receiver_email = "Mrutunjaya.Sahoo@biourja.com"

firefox_driver_path = os.getcwd() + '\\geckodriver.exe'
# chrome_driver_path =  r"S:\IT Dev\Production_Environment\chromedriver\chromedriver.exe"
download_path = os.getcwd() + '\\contingencies_file_download\\'

log_file_location = os.getcwd() +'\\' + 'logs' + '\\' +'PJM_AUCTION_CONTINGENCIES_LOG.txt'
if os.path.isfile(log_file_location):
    os.remove(log_file_location)

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] - %(message)s',
    filename=log_file_location)

testing = False

# SNOWFLAKE DESTINATION
databasename = "POWERDB_DEV"
schemaname = "ISO"
tablename = "PJM_AUCTION_CONTINGENCIES"

def get_auctiontype_max_date(auctiontype, databasename, schemaname, tablename, datecolumn):

    sql = '''
    select max({}) as MAX_DATE from {}
    where auction_type = '{}'
    '''.format(datecolumn, databasename+"."+schemaname+"."+tablename, auctiontype)

    try:
        conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='ISO')
        conn.cursor().execute("USE WAREHOUSE QUANT_WH")

        cs = conn.cursor()
        cs.execute(sql)

        df = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])
        conn.close()
        
        maxdate = df["MAX_DATE"].max()
        return maxdate if maxdate is not np.nan else None

    except Exception as e:
        print(f"Exception caught {e} in fetching max date for {auctiontype}")
        logging.exception(f'Exception caught during execution: {e}')
        raise e

def change_model_idx(auction_type, databasename, schemaname, tablename):# changes model_ids by -1 of previous data of particular auction_type

    sql = '''
    select distinct model_idx as MODEL_IDX from {} where auction_type = '{}'
    '''.format(databasename+"."+schemaname+"."+tablename, auction_type)

    try:
        conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='ISO')
        conn.cursor().execute("USE WAREHOUSE QUANT_WH")

        cs = conn.cursor()
        cs.execute(sql)

        df = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])
        vals = df["MODEL_IDX"].values
        vals.sort()

        # print(vals)
        for val in vals:
            sql = '''
            update {} set model_idx = {} where model_idx = {} and auction_type = '{}'
            '''
            cs.execute(sql.format(databasename+"."+schemaname+"."+tablename, val-1, val, auction_type))

        sql = '''
        select count(*) from {} where model_idx = -1 and auction_type = '{}'
        '''.format(databasename+"."+schemaname+"."+tablename, auction_type)

        cs.execute(sql)
        df = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])
        conn.close()
        
        return df["COUNT(*)"].max()

    except Exception as e:
        print(f"Exception caught {e} during execution")
        logging.exception(f'Exception caught during execution: {e}')
        raise e


if __name__ == "__main__":
    driver = None
    starttime=datetime.now()
    logging.info('Execution Started')
    rows=0
    log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
    logging.info('Start work at {} ...'.format(starttime.strftime('%Y-%m-%d %H:%M:%S')))
    try:
        bu_alerts.bulog(process_name="PJM_AUCTION_CONTINGENCIES",database='POWERDB_DEV',status='Started',table_name = databasename +'.'+ schemaname +'.'+ tablename, row_count=rows, log=log_json, warehouse='QUANT_WH',process_owner='radha/rahul')
        # annual_max_date = get_auctiontype_max_date("ANNUAL", databasename, schemaname, tablename, "RELEASE_DATE")
        # monthly_max_date = get_auctiontype_max_date("MONTHLY", databasename, schemaname, tablename, "RELEASE_DATE")
        annual_max_date = None
        monthly_max_date = None
        # username='7139299183'
        # password='Biourja_1409'
        with requests.Session() as s:
        # webdriver.Firefox(executable_path="D:\\python_practice\geckodriver.exe")
        # browser = webdriver.Firefox(firefox_profile=fp, options=options, executable_path=executable_path)
        
            firefoxOptions = webdriver.FirefoxOptions()
            prefs = {'download.default_directory' : download_path}
            # firefoxOptions.add_argument('prefs', prefs)
            driver = webdriver.Firefox(executable_path=firefox_driver_path, options=firefoxOptions)
            #open the url into web browser
            driver.get('https://pjm.com/Login.aspx')
            time.sleep(15)
            USERNAME = 'SSHARMABUP'
            PASSWORD = 'Houston?1234'
            username = driver.find_element_by_id("body_0_ctl00_txtUserName")
            password = driver.find_element_by_id("body_0_ctl00_txtPassword")
            checkbox = driver.find_element_by_id("body_0_ctl00_chkRememberMe")
            username.send_keys(USERNAME)
            password.send_keys(PASSWORD)
            checkbox.send_keys('checked')
            driver.find_element_by_id("body_0_ctl00_btnLogin").click()
            # print('logging in')
            logging.info("logging in")
            time.sleep(10)
            driver.get('https://pjm.com/markets-and-operations/ftr/private-ftr-model-ceii-data.aspx')
            time.sleep(5)
            
            monthly_xpath = "/html/body/form/div[4]/div/div/div[2]/div[1]/article/table[1]/tbody/tr[4]/td[2]/div/span"
            monthly_r_date = driver.find_element_by_xpath(monthly_xpath).text
            monthly_r_date = datetime.strptime(monthly_r_date, "%m.%d.%Y")

            annual_xpath = "/html/body/form/div[4]/div/div/div[2]/div[1]/article/table[2]/tbody/tr[4]/td[2]/div/span"
            annual_r_date = driver.find_element_by_xpath(annual_xpath).text
            annual_r_date = datetime.strptime(annual_r_date, "%m.%d.%Y")

            if annual_max_date is None or annual_r_date.date() > annual_max_date or testing:
                # annual_link = driver.find_element_by_xpath("/html/body/form/div[4]/div/div/div[2]/div[1]/article/table[2]/tbody/tr[4]/td[1]/a").get_attribute("href")
                annual_link = WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH, "/html/body/form/div[4]/div/div/div[2]/div[1]/article/table[2]/tbody/tr[4]/td[1]/a"))).get_attribute("href")
                driver.get(annual_link)
                # annualdata = (driver.find_element_by_xpath("/html/body/pre").text).split("contingency")
                annualdata = (WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH, "/html/body/pre"))).text).split("contingency")
                # r = s.get('https://pjm.com/-/media/markets-ops/ftr/private/annual-pjm-contingency-list-psse-compatible.ashx')
                        #    https://pjm.com/-/media/markets-ops/ftr/private/annual-pjm-contingency-list-psse-compatible.ashx
                # annualdata = r.text.split("contingency")
            else:
                annualdata = None

            if monthly_max_date is None or monthly_r_date.date() > monthly_max_date or testing:
                driver.get('https://pjm.com/markets-and-operations/ftr/private-ftr-model-ceii-data.aspx')
                time.sleep(5)
                # monthly_link = driver.find_element_by_xpath("/html/body/form/div[4]/div/div/div[2]/div[1]/article/table[1]/tbody/tr[4]/td[1]/a").get_attribute("href")
                monthly_link = WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH, "/html/body/form/div[4]/div/div/div[2]/div[1]/article/table[1]/tbody/tr[4]/td[1]/a"))).get_attribute("href")
                driver.get(monthly_link)
                # monthlydata = (driver.find_element_by_xpath("/html/body/pre").text).split("contingency")
                monthlydata = (WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH, "/html/body/pre"))).text).split("contingency")
                # r = s.get('https://www.pjm.com/-/media/markets-ops/ftr/private/pjm-contingency-list-psse-compatible.ashx?la=en')
                # monthlydata = r.text.split("contingency")
                time.sleep(5)
            else:
                monthlydata = None
        logging.info("annualdata and monthlydata feteched, closing driver")
        driver.quit()
        dfs = []
        emaildf = []
        for i in range(2):
            if i == 0:
                data = annualdata
                auction_type = "ANNUAL"
            else:
                data = monthlydata
                auction_type = "MONTHLY"
            if data is not None:
                logging.info("Changing model_IDX")
                # LOOP THROUGH SNOWFLAKE MODEL_IDX VALUES
                oldrows = change_model_idx(auction_type, databasename, schemaname, tablename) # changes model_ids by -1 of previous data of particular auction_type
                # return count of rows with model_idx = -1
                logging.info("model_idx Changed")

                cons = []
                for info in data:
                    if "end" in info:
                        cons.append(info.split("\n")[:-2])

                dflist = []
                for con in cons:
                    # if con[0] == 'open branch from bus   1334  to bus 1004  ckt 1    /   SORENSON345 KV  SOR-DUM1':
                    #     print(con[0])
                    # print(f"con is {con}")
                    con_info = con[0]
                    # print(f"con_info is {con_info}")
                    bu_iso = "PJMISO"
                    match = re.search(r"\d{8}", con_info) #Searches for 8 digits
                    # print(f"match is {match}")
                    con_id = con_info[match.start():match.end()]
                    # print(f"con_id is {con_id}")
                    con_info = con_info[match.end():]
                    # print(f"now con_info is {con_info}")
                    match = re.search(r"\S{2,}", con_info)
                    # print(f"now match is {match}")
                    con_info = con_info[match.start():]
                    # print(f"now con_info is {con_info}")
                    con_name = re.sub(r"\s+$", "", con_info)
                    # print(f"con_name is {con_name}")
                    con = con[1:]
                    # print(f"now con is {con}")
                    for branch in con:
                        # print(f"branch is {branch}")
                        starts = [v.start() for v in re.finditer(r"\d+", branch)][:3]
                        # print(f"starts is {starts}")
                        ends = [re.search(r"\W", branch[start:]).start() + start for start in starts]
                        # print(f"ends is {ends}")
                        vals = [branch[starts[i]:ends[i]] for i in range(3)]
                        # print(f"vals is {vals}")
                        branch = branch[ends[2]:]
                        # print(f"now branch is {branch}")
                        match = re.search(r"\w{2,}", branch)
                        # print(f"now match is {match}")
                        branch = branch[match.start():]
                        # print(f"now branch is {branch}")
                        branch = re.sub(r"\s+$", "", branch)
                        # print(f"now branch is {branch}")
                        dfitem = (bu_iso, con_id, con_name, vals[0], vals[1], vals[2], branch)
                        # print(f"dfitem is {dfitem}")
                        dflist.append(dfitem)
                        # print(f"dflist is {dflist}")
                logging.info("Creating Dataframe")
                df = pd.DataFrame(dflist)
                df.columns = ["BU_ISO", "CONTINGENCY_ID", "CONTINGENCY_NAME", "FROM_BUS", "TO_BUS",
                            "CIRCUIT_ID", "CONTINGENT_BRANCH_NAME"]
                # Adding other columns
                df["AUCTION_TYPE"] = auction_type
                df["RELEASE_DATE"] = annual_r_date if i == 0 else monthly_r_date
                df["MODEL_IDX"] = 0
                dfs.append(df)
                emaildf.append((auction_type, len(df), oldrows, str(datetime.now().date())))
        logging.info("data frame created")
        # print(pd.concat(dfs))
        #%%
        msgbody = ''
        rows = len(dfs)
        if len(dfs) > 0:
            finaldf = pd.concat(dfs) # Combines all dataframes into single one
            finaldf["INSERTED_DATE"] = datetime.now()
            pklist = ["BU_ISO", "CONTINGENCY_ID", "FROM_BUS", "TO_BUS", "CIRCUIT_ID", "RELEASE_DATE", "AUCTION_TYPE"]
            logging.info("loadding data into sf")
            res = load_df_to_sf(finaldf, databasename, schemaname, tablename, pklist, uploadinsert=False)

            to_addr = ["ftr@biourja.com"]
            mailsubj = 'PJMISO AUCTION CONTINGENCIES -> Snowflake'
            df = pd.DataFrame(emaildf, columns=["Auction Type", "MODEL_IDX 0", "MODEL_IDX -1", "Insert Date"])
            
            msgbody = '<table style="border-spacing: 15px">'    
            msgbody = msgbody + '<tr>'
            
            for x in list(df.columns):
                msgbody = msgbody + '<td style="font-weight: bold; text-align:left; background-color: light grey">{}</td>'.format(x)
            msgbody = msgbody + '</tr>'

            for i, x in df.iterrows():
                msgbody = msgbody + '<tr>'
                for y in list(df.columns):
                    msgbody = msgbody + '<td style="text-align:left">{}</td>'.format(x[y])
                msgbody = msgbody + '</tr>'

            msgbody = msgbody + '</table>'
            # sendmail(to_addr, mailsubj, msgbody)
        else:
            print("Up to date")
            logging.info("Up to date")
        logging.info("Sending mails now")
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name="PJM_AUCTION_CONTINGENCIES",database='POWERDB_DEV',status='Completed',table_name = databasename +'.'+ schemaname +'.'+ tablename, row_count=rows, log=log_json, warehouse='QUANT_WH',process_owner='radha/rahul') 
        logging.info('Execution Done')
        bu_alerts.send_mail(
            receiver_email = receiver_email,
            mail_subject ='JOB SUCCESS - PJM_AUCTION_CONTINGENCIES',
            mail_body = 'PJM_AUCTION_CONTINGENCIES completed successfully, Attached logs'+ msgbody,
            attachment_location = log_file_location
        )   
    except Exception as e:     
        print("Exception caught during execution: ",e)
        logging.exception(f'Exception caught during execution: {e}')
        if driver is not None:
            driver.quit()
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name="PJM_AUCTION_CONTINGENCIES",database='POWERDB_DEV',status='Failed',table_name = databasename +'.'+ schemaname +'.'+ tablename, row_count=0, log=log_json, warehouse='QUANT_WH',process_owner='radha/rahul') 
        bu_alerts.send_mail(
            receiver_email = receiver_email,
            mail_subject='JOB FAILED - PJM_AUCTION_CONTINGENCIES',
            mail_body='PJM_AUCTION_CONTINGENCIES failed during execution, Attached logs',
            attachment_location = log_file_location
        )
    
    endtime=datetime.now()
    print('Complete work at {} ...'.format(endtime.strftime('%Y-%m-%d %H:%M:%S')))
    print('Total time taken: {} seconds'.format((endtime-starttime).total_seconds()))
    
#%%