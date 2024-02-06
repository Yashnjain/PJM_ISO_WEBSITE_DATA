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
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
# To get credentials used in process
from bu_config import get_config
from tosnowflake import load_df_to_sf


for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)


job_id=np.random.randint(1000000,9999999)
firefox_driver_path = os.getcwd() + '\\geckodriver.exe'
# firefox_driver_path = os.getcwd() + '\\pjm_auctions_flowgates_line_ratings\\geckodriver.exe'

download_path = os.getcwd() + '\\pjm_auctions_flowgates_line_ratings\\contingencies_file_download\\'

log_file_location = os.getcwd() +'\\' + 'logs' + '\\' +'PJM_AUCTION_CONTINGENCIES_LOG.txt'
if os.path.isfile(log_file_location):
    os.remove(log_file_location)

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] - %(message)s',
    filename=log_file_location)

testing = False

def get_auctiontype_max_date(auctiontype, databasename, schemaname, tablename, datecolumn):

    sql = '''
    select max({}) as MAX_DATE from {}
    where auction_type = '{}'
    '''.format(datecolumn, databasename+"."+schemaname+"."+tablename, auctiontype)

    try:
        conn = get_connection(role="OWNER_"+databasename,database=databasename,schema= schemaname)
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
    conn = get_connection(role="OWNER_"+databasename,database=databasename,schema=schemaname)
    try:
        # conn = get_connection(role="OWNER_"+databasename,database=databasename,schema=schemaname)
        conn.cursor().execute("USE WAREHOUSE QUANT_WH")

        cs = conn.cursor()
        cs.execute(sql)

        df = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])
        vals = df["MODEL_IDX"].values
        vals.sort()

        # print(vals)

        sql = '''
        update {} set model_idx = model_idx -1 where auction_type = '{}'
        '''
        cs.execute(sql.format(databasename+"."+schemaname+"."+tablename, auction_type))

        sql = '''
        select count(*) from {} where model_idx = -1 and auction_type = '{}'
        '''.format(databasename+"."+schemaname+"."+tablename, auction_type)

        cs.execute(sql)
        df = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])
        # conn.close()
        
        return df["COUNT(*)"].max()

    except Exception as e:
        print(f"Exception caught {e} during execution")
        logging.exception(f'Exception caught during execution: {e}')
        raise e
    finally:
        conn.close()

if __name__ == "__main__":
    driver = None
    starttime=datetime.now()
    logging.info('Execution Started')
    rows=0
    try:
        credential_dict = get_config('PJMISO website data','PJM_AUCTION_CONTINGENCIES')
        databasename = credential_dict['DATABASE']
        # databasename = "POWERDB_DEV"
        schemaname = credential_dict['TABLE_SCHEMA']
        tablename = credential_dict['TABLE_NAME']
        print(tablename)
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        logging.info('Start work at {} ...'.format(starttime.strftime('%Y-%m-%d %H:%M:%S')))
        bu_alerts.bulog(process_name=credential_dict['TABLE_NAME'],database=databasename,status='Started',table_name = databasename +'.'+ schemaname +'.'+ tablename, row_count=rows, log=log_json, warehouse='QUANT_WH',process_owner=credential_dict['IT_OWNER'])
        annual_max_date = get_auctiontype_max_date("ANNUAL", databasename, schemaname, tablename, "RELEASE_DATE")
        monthly_max_date = get_auctiontype_max_date("MONTHLY", databasename, schemaname, tablename, "RELEASE_DATE")
        
        # with requests.Session() as s:
            # chromeOptions = webdriver.ChromeOptions()
            # prefs = {'download.default_directory' : download_path}
            # #set the download path for annual auction file
            # chromeOptions.add_experimental_option('prefs', prefs)
        options = Options()
        mime_types = ['text/plain', 'application/vnd.ms-excel', 'text/csv', 'application/csv', 'text/comma-separated-values',
				  'application/download', 'application/octet-stream', 'binary/octet-stream', 'application/binary', 'application/x-unknown']
        fp = webdriver.FirefoxProfile()
        fp.set_preference("browser.download.folderList", 2)
        fp.set_preference("browser.download.manager.showWhenStarting", False)
        fp.set_preference("browser.download.dir", download_path)
        fp.set_preference("browser.helperApps.neverAsk.saveToDisk",
                        ",".join(mime_types))
        fp.set_preference("security.default_personal_cert", "Select Automatically")
        fp.set_preference("security.tls.version.min", 1)
        fp.accept_untrusted_certs = True
        # options.set_preference("browser.download.dir", download_path)
        # prefs = {'download.default_directory' : download_path}
        # firefoxOptions.add_argument('prefs', prefs)
        # binary = FirefoxBinary(r"C:\\Users\\chetan.surwade\\AppData\\Local\\Mozilla Firefox\\firefox.exe")
        binary = FirefoxBinary(r"C:\\Program Files\\Mozilla Firefox\\Firefox.exe")
        driver = webdriver.Firefox(executable_path=firefox_driver_path, options=options,firefox_binary=binary,firefox_profile=fp)  
        # driver = webdriver.Firefox(executable_path=firefox_driver_path, options=options)
        # driver = webdriver.Chrome(executable_path=chrome_driver_path, options=chromeOptions)       # chrome_options=chromeOptions)
        #open the url into web browser
        print(credential_dict['SOURCE_URL'].split(';')[0])
        driver.get(credential_dict['SOURCE_URL'].split(';')[0])
        time.sleep(15)
        # USERNAME = 'SSHARMABUP'
        # PASSWORD = 'Houston?1234'
        username = driver.find_element_by_id("body_0_ctl00_txtUserName")
        password = driver.find_element_by_id("body_0_ctl00_txtPassword")
        checkbox = driver.find_element_by_id("body_0_ctl00_chkRememberMe")
        username.send_keys(credential_dict['USERNAME'])	
        password.send_keys(credential_dict['PASSWORD'])
        checkbox.send_keys('checked')
        driver.find_element_by_id("body_0_ctl00_btnLogin").click()
        time.sleep(20)
        print('logging in')
        logging.info("logging in")
        time.sleep(20)
        driver.get(credential_dict['SOURCE_URL'].split(';')[1])
        time.sleep(10)
        
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
            annualdata = (WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH, "/html/body/pre"))).text).split("contingency")
        else:
            annualdata = None

        if monthly_max_date is None or monthly_r_date.date() > monthly_max_date or testing:
            driver.get(credential_dict['SOURCE_URL'].split(';')[1])
            time.sleep(5)
            # monthly_link = driver.find_element_by_xpath("/html/body/form/div[4]/div/div/div[2]/div[1]/article/table[1]/tbody/tr[4]/td[1]/a").get_attribute("href")
            monthly_link = WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH, "/html/body/form/div[4]/div/div/div[2]/div[1]/article/table[1]/tbody/tr[4]/td[1]/a"))).get_attribute("href")
            driver.get(monthly_link)
            # monthlydata = (driver.find_element_by_xpath("/html/body/pre").text).split("contingency")
            monthlydata = (WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH, "/html/body/pre"))).text).split("contingency")
            time.sleep(5)
        else:
            monthlydata = None
        if annualdata or monthlydata:
            try:
                os.remove(download_path+'\\annual-pjm-contingency-flowgate-list.xlsx')
            except Exception:
                logging.warning('Problem deleting flowgate id file.')
            driver.get(credential_dict['SOURCE_URL'].split(';')[1])
            flowgate_list_elem_path='/html/body/form/div[4]/div/div/div[2]/div[1]/article/table[2]/tbody/tr[3]/td[1]/a'
            flowgate_list_elem = WebDriverWait(driver, 240).until(EC.presence_of_element_located((By.XPATH, flowgate_list_elem_path)))
            flowgate_list_elem.click()
            time.sleep(30)
            while not os.path.exists(f'{download_path}\\annual-pjm-contingency-flowgate-list.xlsx'):
                time.sleep(10)
            fg_df = pd.read_excel(f'{download_path}\\annual-pjm-contingency-flowgate-list.xlsx',header=1)
            fg_df = fg_df.drop(['Real Time Contingency ID'],axis=1)
            fg_df.columns = ['ID','NAME']
            fg_df = fg_df.dropna().reset_index(drop=True)
            fg_df['ID'] = fg_df.ID.astype('int').astype('str')
            logging.info("annualdata and monthlydata feteched")
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
                    con_info = con[0]
                    bu_iso = "PJMISO"
                    match = re.search(r"\d{8}", con_info) #Searches for 8 digits
                    con_id = con_info[match.start():match.end()]
                    con_info = con_info[match.end():]
                    match = re.search(r"\S{2,}", con_info)
                    con_info = con_info[match.start():]
                    con_name = re.sub(r"\s+$", "", con_info)
                    con = con[1:]
                    for branch in con:
                        starts = [v.start() for v in re.finditer(r"\d+", branch)][:3]
                        ends = [re.search(r"\W", branch[start:]).start() + start for start in starts]
                        vals = [branch[starts[i]:ends[i]] for i in range(3)]
                        branch = branch[ends[2]:]
                        match = re.search(r"\w{2,}", branch)
                        branch = branch[match.start():]
                        branch = re.sub(r"\s+$", "", branch)
                        dfitem = (bu_iso, con_id, con_name, vals[0], vals[1], vals[2], branch)
                        dflist.append(dfitem)
                logging.info("Creating Dataframe")
                df = pd.DataFrame(dflist)
                df.columns = ["BU_ISO", "CONTINGENCY_ID", "CONTINGENCY_NAME", "FROM_BUS", "TO_BUS",
                            "CIRCUIT_ID", "CONTINGENT_BRANCH_NAME"]
                # Adding other columns
                flowgate_df = df[df.CONTINGENCY_ID.isin(fg_df['ID'])]
                non_flowgate_df = df[~df.CONTINGENCY_ID.isin(fg_df['ID'])]
                non_flowgate_df = non_flowgate_df.reset_index(drop=True)
                flowgate_df_updated = pd.merge(flowgate_df,fg_df,left_on='CONTINGENCY_ID',right_on='ID',how='left')[['BU_ISO','CONTINGENCY_ID','NAME','FROM_BUS','TO_BUS','CIRCUIT_ID','CONTINGENT_BRANCH_NAME']].reset_index(drop=True)
                flowgate_df_updated.columns = df.columns
                df = pd.concat([flowgate_df_updated,non_flowgate_df]).reset_index(drop=True)
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
        bu_alerts.bulog(process_name=credential_dict['TABLE_NAME'],database=databasename,status='Completed',table_name = databasename +'.'+ schemaname +'.'+ tablename, row_count=rows, log=log_json, warehouse='QUANT_WH',process_owner=credential_dict['IT_OWNER']) 
        logging.info('Execution Done')
        bu_alerts.send_mail(
            receiver_email = credential_dict['EMAIL_LIST'],
            # receiver_email= 'priyanka.solanki@biourja.com,radha.waswani@biourja.com',
            mail_subject ='JOB SUCCESS - {}'.format(credential_dict['TABLE_NAME']),
            mail_body = '{} completed successfully, Attached logs'.format(credential_dict['TABLE_NAME'])+ msgbody,
            attachment_location = log_file_location
        )
    except Exception as e:     
        print("Exception caught during execution: ",e)
        logging.exception(f'Exception caught during execution: {e}')
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name=credential_dict['TABLE_NAME'],database='POWERDB',status='Failed',table_name = databasename +'.'+ schemaname +'.'+ tablename, row_count=0, log=log_json, warehouse='QUANT_WH',process_owner=credential_dict['IT_OWNER']) 
        bu_alerts.send_mail(
            receiver_email = credential_dict['EMAIL_LIST'],
            # receiver_email= 'priyanka.solanki@biourja.com,radha.waswani@biourja.com',
            mail_subject='JOB FAILED - {}'.format(credential_dict['TABLE_NAME']),
            mail_body='{} failed during execution, Attached logs'.format(credential_dict['TABLE_NAME']),
            attachment_location = log_file_location
        )
        sys.exit(1)
    # if driver is not None:
    #     driver.quit()
    endtime=datetime.now()
    print('Complete work at {} ...'.format(endtime.strftime('%Y-%m-%d %H:%M:%S')))
    print('Total time taken: {} seconds'.format((endtime-starttime).total_seconds()))