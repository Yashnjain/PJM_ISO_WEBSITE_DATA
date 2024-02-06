from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
import time
from datetime import date,datetime
import logging
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.firefox import GeckoDriverManager
import os
from bu_config import get_config
import bu_alerts
import numpy as np
import pandas as pd
import bu_snowflake
from snowflake.connector.pandas_tools import write_pandas
from bs4 import BeautifulSoup
import requests
from lxml import html
from datetime import datetime, timedelta
import re
import xlwings


def remove_existing_files(files_location):
    """_summary_

    Args:
        files_location (_type_): _description_

    Raises:
        e: _description_
    """           
    logger.info("Inside remove_existing_files function")
    try:
        files = os.listdir(files_location)
        if len(files) > 0:
            for file in files:
                os.remove(files_location + "\\" + file)
            logger.info("Existing files removed successfully")
        else:
            print("No existing files available to reomve")
        print("Pause")
    except Exception as e:
        logger.info(e)
        raise e


def download_wait(directory,filename,nfiles = None):
    try:
        seconds = 0
        dl_wait = True
        while dl_wait and seconds < 1000:
            time.sleep(1)
            dl_wait = False
            files = os.listdir(directory)
            if nfiles and len(files) != nfiles:
                dl_wait = True
            for fname in files:
                print(fname)
                if fname.endswith('.crdownload'):
                    dl_wait = True
                elif fname.endswith('.tmp'):
                    dl_wait = True
                elif fname.endswith(filename+'.part'):
                    dl_wait = True
            seconds += 1
        return seconds            
    except Exception as e:
        logger.exception(f"error occurred : {e}")
        raise e


def download_files(dataframe,files_location,driver,auction_type):    
    try:
        if len(dataframe)>0:
            driver.set_page_load_timeout(60)
            for i in range(0,len(dataframe)):
                print(i)
                logging.info(f'current i is {i}')
                returnvalue=requests.get(dataframe['OnpeakLink'][i]).status_code
                filename=dataframe['OnpeakLink'][i].split('/')[-1].split('.')[0]
                if returnvalue==200:
                    try:
                        driver.get(dataframe['OnpeakLink'][i])
                    except:  
                        download_wait(files_location,filename)
                else:
                    print(f"link broken for file - {filename}")
                returnvalue=requests.get(dataframe['OffpeakLink'][i]).status_code
                filename=dataframe['OffpeakLink'][i].split('/')[-1].split('.')[0]
                if returnvalue==200:
                    try:
                        driver.get(dataframe['OffpeakLink'][i])
                    except:  
                        download_wait(files_location,filename)
                    # download_wait(files_location,filename)
                else:
                    print(f"link broken for file - {filename}")

                if 'OffpeakLink2' in dataframe.columns:
                    returnvalue=requests.get(dataframe['OffpeakLink2'][i]).status_code
                    filename=dataframe['OffpeakLink2'][i].split('/')[-1].split('.')[0]
                    if returnvalue==200:
                        try:
                            if dataframe['OffpeakLink2'][i]!=None:
                                driver.get(dataframe['OffpeakLink2'][i])
                        except:  
                            download_wait(files_location,filename)
                    else:
                        print(f"link broken for file - {filename}")
        else:
            logging.info(f"no new data found as per max date of {auction_type}")
                                
    except Exception as e:
        logger.exception(f"error occurred : {e}")
        raise e


def get_end_of_month(month):
    try:
        month = month + timedelta(days=31)
        month = month.replace(day=1)
        month = month - timedelta(days=1)
        return month
    except Exception as e:
        logger.exception(f"error occurred : {e}")
        raise e


def calc_start_end_date_by_period(period: str, year: str, result_month: int):
    try:
        quater_start_mappings = {'Q1': 'JUN',
                                'Q2': 'SEP', 'Q3': 'DEC', 'Q4': 'MAR'}
        quater_end_mappings = {'Q1': 'AUG', 'Q2': 'NOV', 'Q3': 'FEB', 'Q4': 'MAY'}
        start_month, end_month = '', ''
        if 'Q' in period:
            start_month = quater_start_mappings[period]
            end_month = quater_end_mappings[period]
        else:
            period_split = period.split('-')
            start_month = period_split[0]
            end_month = period_split[-1]

        end_month_num = datetime.strptime(end_month, "%b").month
        # logger.info(start_month_num, end_month_num, result_month, year)
        if end_month_num < result_month:
            start_date = datetime.strptime(
                "{}{}01".format(int(year)+1, start_month), "%Y%b%d")
        else:
            start_date = datetime.strptime(
                "{}{}01".format(int(year), start_month), "%Y%b%d")
        end_date = get_end_of_month(start_date.replace(month=end_month_num))
        return [start_date, end_date]
    except Exception as e:
        logger.exception(f"error occurred : {e}")
        raise e


def get_max_date(conn,auction_type):
    try:
        # conn = bulog.get_connection()
        cs = conn.cursor()
        cs.execute(f"select max(POSTED_DATE) as MAX_DATE from {database}.{schema}.{table_name} where auction_type ='{auction_type.split(' ')[0]}'")
        df = pd.DataFrame.from_records(
            iter(cs), columns=[x[0] for x in cs.description])
        return df["MAX_DATE"].max() if df["MAX_DATE"].max() is not np.nan else None

    except Exception as e:
        logger.exception(e)
        return None


def login_and_download(conn,no_of_rows,auction_type):  
    '''This function downloads log in to the website'''
    try:    
            global event
            event = False
            logging.info('Accesing website')
            driver.get(source_url)
            time.sleep(5)
            WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".username"))).send_keys(username)
            time.sleep(1)
            WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='password']"))).send_keys(password)
            time.sleep(1)
            WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[value='Submit']"))).click()
            time.sleep(5)
            try:
                WebDriverWait(driver, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a[href='/markets-and-operations/ftr/private-ftr-model-ceii-data']"))).click()
                time.sleep(5)
            except:
                pass
            soup = BeautifulSoup(driver.page_source, "html.parser")
            tables = soup.find_all('table')
            base_url="https://www.pjm.com"
            for table in tables:
                    if auction_type in table.text:
                        records = []
                        columns = []
                        for tr in table.findAll("tr"):
                            ths = tr.findAll("th")
                            if ths != []:
                                for each in ths:
                                    columns.append((each.text).replace('\n',''))
                            else:
                                trs = tr.findAll("td")
                                # trs= trs.findAll("a")
                                record = []                  
                                try:
                                    record.append(trs[0].text.replace('\n',''))
                                    record.append(trs[0].findAll('a')[0]['href'])
                                    # record.append(trs[0].findAll('a')[1].text)
                                    record.append(trs[0].findAll('a')[1]['href'])
                                    try:
                                        record.append(trs[0].findAll('a')[2]['href'])
                                        record.append(trs[1].text.replace('\n',''))
                                        event = True
                                    except:
                                        record.append(None)
                                        record.append(trs[1].text.replace('\n',''))
                                except:
                                    text = (each.text).replace('\n','')
                                    record.append(text)
                                records.append(record)

                        columns.insert(1, 'OnpeakLink')
                        columns.insert(2, 'OffpeakLink')
                        if event:
                            columns.insert(3, 'OffpeakLink2')
                        else:
                            columns.insert(3, 'nolinks') 
                        if auction_type=='Annual FTR Auction':
                         Annual_df = pd.DataFrame(data=records, columns = columns)
                         Annual_df['OnpeakLink'] =base_url + Annual_df['OnpeakLink']
                         Annual_df['OffpeakLink'] =base_url + Annual_df['OffpeakLink']
                         if 'OffpeakLink2' in Annual_df.columns:
                            for i in range(len(Annual_df)):
                                if Annual_df['OffpeakLink2'][i]!=None:
                                    Annual_df['OffpeakLink2'][i] =base_url + Annual_df['OffpeakLink2'][i]
                                else:
                                    print("None in dataframe") 
                         Annual_df["Date"]=Annual_df["Date"].astype('datetime64[ns]').astype(str)
                         max_date = get_max_date(conn,auction_type)
                         if max_date!=None:
                            Annual_df=Annual_df.loc[Annual_df["Date"]>str(max_date)]
                            Annual_df.reset_index(inplace=True, drop=True)
                         download_files(Annual_df,files_location,driver,auction_type)
                        elif auction_type=='Monthly FTR Auction':
                         Monthlydf = pd.DataFrame(data=records, columns = columns)
                         Monthlydf['OnpeakLink'] =base_url + Monthlydf['OnpeakLink']
                         Monthlydf['OffpeakLink'] =base_url + Monthlydf['OffpeakLink']
                         if 'OffpeakLink2' in Monthlydf.columns:
                            for i in range(len(Monthlydf)):
                                if Monthlydf['OffpeakLink2'][i]!=None:
                                    Monthlydf['OffpeakLink2'][i] =base_url + Monthlydf['OffpeakLink2'][i]
                                else:
                                    print("None in dataframe") 
                         Monthlydf["Date"]=Monthlydf["Date"].astype('datetime64[ns]').astype(str)
                         max_date = get_max_date(conn,auction_type)
                         if max_date!=None:
                            Monthlydf=Monthlydf.loc[Monthlydf["Date"]>str(max_date)]
                            Monthlydf.reset_index(inplace=True, drop=True)
                         download_files(Monthlydf,files_location,driver,auction_type)
                        elif auction_type=='Long-Term FTR Auction':
                         LongTermdf = pd.DataFrame(data=records, columns = columns)
                         LongTermdf['OnpeakLink'] =base_url + LongTermdf['OnpeakLink']
                         LongTermdf['OffpeakLink'] =base_url + LongTermdf['OffpeakLink']
                         if 'OffpeakLink2' in LongTermdf.columns:
                            for i in range(len(LongTermdf)):
                                if LongTermdf['OffpeakLink2'][i]!=None:
                                    LongTermdf['OffpeakLink2'][i] =base_url + LongTermdf['OffpeakLink2'][i]
                                else:
                                    print("None in dataframe") 
                         LongTermdf["Date"]=LongTermdf["Date"].astype('datetime64[ns]').astype(str)
                         max_date = get_max_date(conn,auction_type)
                         if max_date!=None:
                            LongTermdf=LongTermdf.loc[LongTermdf["Date"]>str(max_date)]
                            LongTermdf.reset_index(inplace=True, drop=True)
                         download_files(LongTermdf,files_location,driver,auction_type) 
   
                        filesToUpload = os.listdir(os.getcwd() + "\\download")
                        if len(filesToUpload)>0:
                            for file in filesToUpload:  
                                df=load_auction_sensitives(auction_type,file) 
                                no_of_rows=insert_to_sf(df,conn,no_of_rows)
                            remove_existing_files(files_location)
                        else:
                            logging.info(f"NO FILES AVAILABLE TO UPLOAD IN DOWNLOAD FOLDER")    
                        break    
            
            return no_of_rows    
    except Exception as e:
        logger.exception(f"error occurred : {e}")
        delete_query=f'''delete from {database}.{schema}.{table_name} where INSERTED_DATE>=current_date()'''
        cur = conn.cursor()
        cur.execute(delete_query)
        raise e


def insert_to_sf(final_df,conn,no_of_rows):        
    try:
        logger.info("inserting data")
        success, nchunks, nrows, _ = write_pandas(conn, final_df, table_name)
        no_of_rows+=nrows
        return no_of_rows 
    except Exception as e:
        logger.exception(f"error occurred : {e}")
        raise(e)


def load_auction_sensitives(auction_type,file):
    try:
        df = pd.read_csv(files_location+"\\"+file)
        posted_date=re.sub(r"\D", "", df.columns[0])
        x=datetime.strptime(posted_date,"%Y%m%d")
        posted_date=datetime.strftime(x,"%m-%d-%Y")
        auction_name=df[df.columns[0]][0].split("-")[1].strip()
        df.insert(0,'AUCTION_NAME',auction_name)
        df.insert(len(df.columns)-1,'POSTED_DATE',posted_date)
        df.insert(len(df.columns)-1,'INSERTED_DATE',str(datetime.now()))
        df.columns = ['AUCTION_NAME','AUCTION_ROUND', 'PERIOD_TYPE', 'PEAK_TYPE', 'CONSTRAINT_NAME','CONTINGENCY_NAME','DIRECTION','PNODE_NAME', 'BUS_NAME','POSTED_DATE','INSERTED_DATE','SENSITIVITY_FACTOR']
        df.insert(2,'AUCTION_TYPE',auction_type.split(' ')[0])
        df.drop(0,inplace=True)
        df.drop(1,inplace=True)
        df.reset_index(drop=True,inplace=True)
        df['AUCTION_ROUND']=df['AUCTION_ROUND'].astype(int)
        df['BUS_NAME']=df['BUS_NAME'].astype(int)
        df['SENSITIVITY_FACTOR']=df['SENSITIVITY_FACTOR'].astype(float)
        year=re.sub(r"\D(\D\D)", "", auction_name)
        if auction_type == "Monthly FTR Auction":
                year = re.sub(r"\D", "", auction_name)
        if auction_type == "Annual FTR Auction":
            df["FLOW_START_DATE"] = datetime.strptime(
                "{}0601".format(str(datetime.strftime(datetime.strptime(year.split("/")[0],"%y"),'%Y'))), "%Y%m%d")
            df["FLOW_END_DATE"] = datetime.strptime(
                "{}0531".format(str(datetime.strftime(datetime.strptime(year.split("/")[1],"%y"),'%Y'))), "%Y%m%d")
        elif auction_type == "Long-Term FTR Auction":
            df["FLOW_START_DATE"] = datetime.strptime(
                "{}0601".format(str(datetime.strftime(datetime.strptime(year.split("/")[0],"%y"),'%Y'))), "%Y%m%d")
            df["FLOW_END_DATE"] = datetime.strptime(
                "{}0531".format(str(datetime.strftime(datetime.strptime(year.split("/")[1],"%y"),'%Y'))), "%Y%m%d")
        elif auction_type == "Monthly FTR Auction":
            wb=xlwings.Book(files_location+"\\"+file)
            sheet_name=wb.sheets[0].name
            wb.app.kill()
            result_month = datetime.strptime(
                sheet_name.split('-')[2], '%b').month
            df['START_END_DATE'] = df['PERIOD_TYPE'].apply(
                lambda x: calc_start_end_date_by_period(x, year, result_month))
            df[['FLOW_START_DATE', 'FLOW_END_DATE']] = pd.DataFrame(
                df.START_END_DATE.tolist(), index=df.index)
            df = df.drop(columns='START_END_DATE')
        df["FLOW_START_DATE"] = df["FLOW_START_DATE"].astype('datetime64[ns]').astype(str)
        df["FLOW_END_DATE"] = df["FLOW_END_DATE"].astype('datetime64[ns]').astype(str) 
        df["POSTED_DATE"] = df["POSTED_DATE"].astype('datetime64[ns]').astype(str) 
        df['CONTINGENCY_NAME']=df["CONTINGENCY_NAME"].astype(str)
        return df    
    except Exception as e:
        logger.exception(f'Error {e}')
        raise e


def main():
    try:
        no_of_rows=0
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name=processname,database=database,status='Started',table_name='',
            row_count=no_of_rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)
        remove_existing_files(files_location)
        auction_types = ["Annual FTR Auction","Monthly FTR Auction" ,"Long-Term FTR Auction"]
        for auction_type in auction_types:
                no_of_rows=login_and_download(conn,no_of_rows,auction_type)
        print("done") 
        locations_list.append(logfile)
        bu_alerts.bulog(process_name=processname,database=database,status='Completed',table_name='',
            row_count=no_of_rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)  
        if no_of_rows>0:
            bu_alerts.send_mail(receiver_email = receiver_email,mail_subject =f'JOB SUCCESS - {job_name} and {no_of_rows} rows updated on {format(datetime.now())}',mail_body = f'{job_name} completed successfully, Attached Logs',attachment_location = logfile)
        else:
            bu_alerts.send_mail(receiver_email = receiver_email,mail_subject =f'JOB SUCCESS - {job_name} - data inserted previously and NO NEW DATA FOUND on {format(datetime.now())}',mail_body = f'{job_name} completed successfully, Attached Logs',attachment_location = logfile)
    except Exception as e:
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name= processname,database=database,status='Failed',table_name='',
            row_count=no_of_rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)
        logging.exception(str(e))
        locations_list.append(logfile)
        bu_alerts.send_mail(receiver_email = receiver_email,mail_subject =f'JOB FAILED -{job_name}',mail_body = f'{job_name} failed on {format(datetime.now())}, Attached logs',multiple_attachment_list = logfile)
        driver.quit()



if __name__ == "__main__": 
    try:
        logging.info("Execution Started")
        time_start=time.time()
        #Global VARIABLES

        locations_list=[]
        body = ''
        today_date=date.today()
        # log progress --
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        # logfile = os.getcwd() +"\\logs\\"+'Enverus_Logfile'+str(today_date)+'.txt'
        logfile = os.getcwd() + '\\' + 'logs' + '\\' + 'PJM_ISO_AUCTION_SENSITIVITIES_AUTOMATION_Log_{}.txt'.format(str(today_date))
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s [%(levelname)s] - %(message)s',
            filename=logfile)

        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        logging.info('setting paTH TO download')
        path = os.getcwd() + '\\download'
        logging.info('SETTING PROFILE SETTINGS FOR FIREFOX')
        profile = webdriver.FirefoxProfile()
        profile.set_preference('browser.download.folderList', 2)
        profile.set_preference('browser.download.dir', path)
        profile.set_preference('browser.download.useDownloadDir', True)
        profile.set_preference('browser.download.viewableInternally.enabledTypes', "")
        profile.set_preference('browser.helperApps.neverAsk.saveToDisk','Portable Document Format (PDF), application/pdf')
        profile.set_preference('pdfjs.disabled', True)
        logging.info('Adding firefox profile')
        driver=webdriver.Firefox(executable_path=GeckoDriverManager().install(),firefox_profile=profile)
        directories_created=["download","Logs"]
        for directory in directories_created:
            path3 = os.path.join(os.getcwd(),directory)  
            try:
                os.makedirs(path3, exist_ok = True)
                print("Directory '%s' created successfully" % directory)
            except OSError as error:
                print("Directory '%s' can not be created" % directory)       
        files_location=os.getcwd() + "\\download"
        filesToUpload = os.listdir(os.getcwd() + "\\download")
        # #main variables
        credential_dict = get_config('PJMISO website data','PJM_ISO_AUCTION_SENSITIVITIES')
        receiver_email = credential_dict['EMAIL_LIST']
        # receiver_email='mrutunjaya.sahoo@biourja.com'
        # receiver_email='priyanka.solanki@biourja.com'
        job_name=credential_dict['TABLE_NAME']
        job_id=np.random.randint(1000000,9999999)
        processname = credential_dict['TABLE_NAME']
        process_owner = credential_dict['IT_OWNER']
        username=credential_dict['USERNAME']
        password=credential_dict['PASSWORD'] 
        source_url=credential_dict['SOURCE_URL']
        # current_yr=today_date.year
        # current_month=today_date.strftime("%m")
        # #snowflake variables
        # database = "POWERDB_DEV"
        database = credential_dict['DATABASE']
        schema = credential_dict['TABLE_SCHEMA']
        table_name = credential_dict['TABLE_NAME']
        conn=bu_snowflake.get_connection(
            database  = database,
            schema=schema,
            role =f"OWNER_{database}"
        )
        main()
        conn.close()
        driver.quit()  
        time_end=time.time()
        logging.info(f'It takes {time_start-time_end} seconds to run')
    except Exception as e:
        logging.exception(str(e))
        bu_alerts.send_mail(receiver_email = receiver_email,mail_subject =f'JOB FAILED -{job_name}',mail_body = f'{job_name} failed in __main__, Attached logs',attachment_location = logfile)
        driver.quit()



