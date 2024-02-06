import sys
import logging
import pathlib
import requests
from lxml import html
import pandas as pd
import numpy as np
import os
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from tenacity import Retrying, RetryError, stop_after_attempt, wait_fixed
import bu_alerts
# To get credentials used in process	
from bu_config import get_config
from snowflake.connector.pandas_tools import pd_writer
import functools
import bu_snowflake
from bu_snowflake import get_connection
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException
import time
import datefinder
# %%
primary_key_list = []

# %%

def remove_existing_files(files_location):
    '''
        This function removes the existing files inside the pjm_auction_constraint_download directory

        Params:
            files_location(str): The location of the files to be removed
    '''

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


def get_max_date(auction_type):
    ''''
        This function fetches the data for the last release date from database for the auction type

        Params:
            auction_type(str): The type of auction
    '''

    try:
        conn = get_connection(database=databasename,schema=schemaname,role=role)
        cs = conn.cursor()
        cs.execute("use warehouse quant_wh")
        cs.execute("use database {}".format(databasename))
        cs.execute("use schema {}".format(schemaname))
        cs.execute("select max(release_date) as MAX_DATE from {} where auction_type = '{}'".format(
            tablename, auction_type))
        df = pd.DataFrame.from_records(
            iter(cs), columns=[x[0] for x in cs.description])
        conn.close()
        return df["MAX_DATE"].max() if df["MAX_DATE"].max() is not np.nan else None

    except Exception as e:
        logger.exception(e)
        return None


def get_auction_df(auction_type):

    ''''
        This function fetches AUCTION_NAME,YES_AUCTION_ID,AUCTION_ROUND from  powerdb.pquant.bu_ftr_auction_vw
        for the required auction type

        Params:
            auction_type(str): The type of auction
    '''

    try:
        conn = get_connection(database=databasename,schema=schemaname,role=role)
        cs = conn.cursor()
        cs.execute("use warehouse quant_wh")
        cs.execute("use database powerdb")
        cs.execute("use schema pquant")
        cs.execute(f"""select AUCTION_NAME,YES_AUCTION_ID,AUCTION_ROUND from bu_ftr_auction_vw where AUCTION_DATE = 
            (select max(AUCTION_DATE) from powerdb.pquant.bu_ftr_auction_vw where 
            BU_AUCTION_TYPE = '{auction_type}' and BU_ISO = 'PJMISO')
            and BU_AUCTION_TYPE = '{auction_type}' and BU_ISO = 'PJMISO'""")

        df = pd.DataFrame.from_records(
            iter(cs), columns=[x[0] for x in cs.description])
        conn.close()
        return df

    except Exception as e:
        logger.exception(e)
        return None



def get_end_of_month(month):
    month = month + timedelta(days=31)
    month = month.replace(day=1)
    month = month - timedelta(days=1)
    return month

# %%


def get_annual_ftr_auction():
    '''
        This function downloads Annual data from the pjmiso website thorugh webscraping

        param: None
    '''
    site = credential_dict['SOURCE_URL'].split(';')[0]
    page = requests.get(site)
    tree = html.fromstring(page.content)
    files = []
    max_date = get_max_date("ANNUAL") or datetime(2016, 12, 1)
    logger.info(max_date)
    try:
        for i in range(9, 18):
            path = f"/html/body/form/div[4]/div/div/div[2]/div/article/div/div[1]/div[{i}]/div/div/table//tr[position()]/td[1]/a/@href"
            table = tree.xpath(path)

            index = 0
            for file in table:
                if "results" in file:
                    datexpath = f"/html/body/form/div[4]/div/div/div[2]/div/article/div/div[1]/div[{i}]/div/div/table/tbody/tr[{1+index}]/td[2]"

                    release_date = re.sub(
                        r"\D", "", tree.xpath(datexpath)[0].text)
                    release_date = datetime.strptime(release_date, "%m%d%Y")
                    if max_date is not None and release_date > max_date:
                        filename = [x.title()
                                    for x in file.split("/")[-1].split("-")[:-1]]
                        auction_name = " ".join(filename).split(".")[0]
                        auction_name = "PJMISO " + \
                            auction_name[:4] + "-" + auction_name[5:]
                        files.append((site, file, auction_name, release_date))
                    else:
                        break
                index = index + 1
    except Exception as e:
        print(e)

    return files


def get_longterm_ftr_auction():
    '''
        This function downloads Longterm data from the pjmiso website thorugh webscraping

        param: None
    '''
    site = credential_dict['SOURCE_URL'].split(';')[0]
    page = requests.get(credential_dict['SOURCE_URL'].split(';')[0])
    tree = html.fromstring(page.content)
    path = "/html/body/form/div[4]/div/div/div[2]/div/article/div/div[1]/div[15]/div/div/table//tr[position()]/td[1]/a/@href"
    table = tree.xpath(path)

    files = []
    max_date = get_max_date("LONGTERM") or datetime(2016, 12, 1)
    logger.info(max_date)
    try:
        for i in range(18, 27):
            index = 0
            path = f"/html/body/form/div[4]/div/div/div[2]/div/article/div/div[1]/div[{i}]/div/div/table//tr[position()]/td[1]/a/@href"
            table = tree.xpath(path)
            for file in table:
                if "results" in file:
                    datexpath = f"/html/body/form/div[4]/div/div/div[2]/div/article/div/div[1]/div[{i}]/div/div/table/tbody/tr[{1+index}]/td[2]"
                    if (i == 23 and index==0):
                        datexpath = f"/html/body/form/div[4]/div/div/div[2]/div/article/div/div[1]/div[{i}]/div/div/table/tbody/tr[{1+index}]/td[2]/span"

                    release_date = re.sub(
                        r"\D", "", tree.xpath(datexpath)[0].text)
                    release_date = datetime.strptime(release_date, "%m%d%Y")
                    if max_date is not None and release_date > max_date:
                        filename = [x.title() for x in file.split(
                            "/")[-1].split(".")[0].split("-")]
                        filename.remove("Ftr")
                        filename.remove("Results")
                        auction_name = " ".join(filename)
                        auction_name = "PJMISO " + \
                            auction_name[:4] + "-" + \
                            auction_name[5:14] + "-" + auction_name[15:]
                        files.append((site, file, auction_name, release_date))
                    else:
                        break
                index = index + 1
    except Exception as e:
        print(e)

    return files


def get_monthly_ftr_auction():
    '''
        This function downloads monthly data from the sharepoint location of pjmiso by web scraping 

        param: None
    '''
    try:
        exec_path = 'geckodriver.exe'
        options = Options()
        # options.headless = False
        fp = webdriver.FirefoxProfile()
        mime_types = ['application/pdf', 'text/plain', 'application/vnd.ms-excel', 'text/csv', 'application/csv', 'text/comma-separated-values','application/download', 'application/octet-stream', 'binary/octet-stream', 'application/binary', 'application/x-unknown']
        fp.set_preference("browser.download.folderList", 2)
        fp.set_preference("browser.download.manager.showWhenStarting", False)
        fp.set_preference("browser.download.dir", files_location)
        fp.set_preference("browser.helperApps.neverAsk.saveToDisk",",".join(mime_types))
        fp.set_preference("browser.helperApps.neverAsk.openFile", "application/pdf, application/octet-stream, application/x-winzip, application/x-pdf, application/x-gzip")
        fp.set_preference("pdfjs.disabled", True)
        driver = browser = webdriver.Firefox(firefox_profile=fp, options=options, executable_path=exec_path)
        # Hitting source url for monthly data
        driver.get(credential_dict['SOURCE_URL'].split(';')[1])
        driver.set_page_load_timeout(300)
        time.sleep(10)
        driver.maximize_window()
        # Logging into the website
        driver.find_element(By.ID, "idToken1").send_keys(username)
        driver.find_element(By.ID, "idToken2").send_keys(password)
        driver.find_element(By.ID, "loginButton_0").click()

        time.sleep(30)
        # Clicking on the dropdown button 
        driver.find_element(By.XPATH, "/html/body/div[1]/div/div/div[3]/div/div[2]/div[3]/div[2]/ \
                            div[1]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[6]/span/span/i").click()
        time.sleep(5)
        # Selecting the list from newer to older 
        driver.find_element(By.XPATH,"/html/body/div[1]/div/div/div[6]/div/div/div/div/div/div/div[3] \
                            /div/div/ul/li[2]/button/div/span").click()
        time.sleep(30)
        # Extracting the text to find the latest release date
        dt = driver.find_element(By.XPATH,"/html/body/div[1]/div/div/div[3]/div/div[2]/div[3]/div[2]/div[1]/ \
                            div/div/div/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div/div/div[1]/ \
                            div[1]/div/div/div[2]/div[3]/div").text
        release_date_obj =  datefinder.find_dates(dt)
        release_date = [date for date in release_date_obj][0]

        max_date = get_max_date("MONTHLY") or datetime(2016, 12, 1)
        logger.info(f"Last release date is :{max_date}")
        
        if max_date is not None and release_date > max_date:
            try:
                # Downloading the latest file for the release date 
                file = driver.find_element(By.XPATH,"/html/body/div[1]/div/div/div[3]/div/div[2]/div[3] \
                                           /div[2]/div[1]/div/div/div/div/div[2]/div/div/div/div/div/div \
                                           /div[2]/div/div/div/div/div[1]/div[1]/div/div/div[2]/div[2] \
                                           /div/div/span/span/span/a")
                logger.info(f"Downloading xpath fetched")
                file_name = file.text
                logger.info(f"File name is {file_name}")
                file.click()
                time.sleep(300)
            
                logger.info("Downloading completed")
            except Exception as e:
                logger.exception(e)

            logger.info(f"File downloaded for release date: {str(release_date)}")

            return (files_location + "\\" + file_name, release_date)
        else:
            logger.info(f"No new files available for release date: {str(release_date)}")
            return []
    except Exception as e:
        logger.exception(e)
        raise e
    finally:
        try:
            driver.quit()
        except Exception as e: 
            logger.info("No browser object to quit")
    

def format_df(csv_file, xlsxfile, release_date, auction_type):
    '''
        This function reads data from the CSV file and formats them into final output

        Params:
            csv_file(str)         : CSV file is the path to the CSV file
            xlsxfile(pd.Excelfile): The excel file instance for dataframe 
            release_date(str)     : Release date of the file
            auction_type(str)     : Name of the auction type (MONTHLY, ANNUAL, LONGTERM)
    '''
    try:
        marginal_cols = ['CONSTRAINT_NAME', 'CTG_ID',
                                'PERIOD_TYPE', 'MARGINAL_VALUE']
        limit_cols = ['CONSTRAINT_NAME', 'CTG_ID',
                        'PERIOD_TYPE', 'LIMIT']
        wkndpeakdf = pd.DataFrame()
        wkndlimitdf = pd.DataFrame()
        for sheet_name in xlsxfile.sheet_names:
            if "Binding Constraints" in sheet_name:
                df = pd.read_excel(csv_file, sheet_name=sheet_name)
                df = df.iloc[1:,]
                
                df.columns = ['CONSTRAINT_NAME', 'CTG_ID', 'PERIOD_TYPE', 'ONPEAK_MARGINAL_VALUE', "WEEKEND_ONPEAK_MARGINAL_VALUE",
                    'OFFPEAK_MARGINAL_VALUE','ONPEAK_LIMIT',"WEEKEND_ONPEAK_LIMIT", 'OFFPEAK_LIMIT']

                logger.info(f"Temporary columns renamed for {auction_type}")
            
                
                offpeakdf = df[~df['OFFPEAK_MARGINAL_VALUE'].isnull(
                )][['CONSTRAINT_NAME', 'CTG_ID', 'PERIOD_TYPE', 'OFFPEAK_MARGINAL_VALUE']]
                offpeakdf.columns = marginal_cols
                offpeakdf['BU_PEAKTYPE'] = 'DAILY_OFFPEAK'

                onpeakdf = df[~df['ONPEAK_MARGINAL_VALUE'].isnull(
                )][['CONSTRAINT_NAME', 'CTG_ID', 'PERIOD_TYPE', 'ONPEAK_MARGINAL_VALUE']]
                onpeakdf.columns = marginal_cols
                onpeakdf['BU_PEAKTYPE'] = 'ONPEAK'
                
                offpeaklimitdf = df[~df['OFFPEAK_LIMIT'].isnull(
                )][['CONSTRAINT_NAME', 'CTG_ID', 'PERIOD_TYPE', 'OFFPEAK_LIMIT']]
                offpeaklimitdf.columns = limit_cols
                offpeaklimitdf['BU_PEAKTYPE'] = 'DAILY_OFFPEAK'

                onpeaklimitdf = df[~df['ONPEAK_LIMIT'].isnull(
                            )][['CONSTRAINT_NAME', 'CTG_ID', 'PERIOD_TYPE', 'ONPEAK_LIMIT']]
                onpeaklimitdf.columns = limit_cols
                onpeaklimitdf['BU_PEAKTYPE'] = 'ONPEAK'

                wkndpeakdf = df[~df['WEEKEND_ONPEAK_MARGINAL_VALUE'].isnull(
                )][['CONSTRAINT_NAME', 'CTG_ID', 'PERIOD_TYPE', 'WEEKEND_ONPEAK_MARGINAL_VALUE']]
                wkndpeakdf.columns = marginal_cols
                wkndpeakdf['BU_PEAKTYPE'] = 'WEEKEND_ONPEAK'

                wkndlimitdf = df[~df['WEEKEND_ONPEAK_LIMIT'].isnull(
                )][['CONSTRAINT_NAME', 'CTG_ID', 'PERIOD_TYPE', 'WEEKEND_ONPEAK_LIMIT']]
                wkndlimitdf.columns = limit_cols
                wkndlimitdf['BU_PEAKTYPE'] = 'WEEKEND_ONPEAK'

                margin_value_df = pd.concat([onpeakdf, wkndpeakdf, offpeakdf])
                limit_df = pd.concat([onpeaklimitdf, wkndlimitdf, offpeaklimitdf])
                df = pd.merge(margin_value_df, limit_df, on=['CONSTRAINT_NAME','CTG_ID','PERIOD_TYPE','BU_PEAKTYPE'])

                auction_round = int(sheet_name[-1]) if auction_type != 'MONTHLY' else 1
                auction_df = get_auction_df(auction_type)
                auction_df = auction_df[auction_df['AUCTION_ROUND'] == auction_round]
                auction_name = auction_df.iloc[0][0]
                df["BU_ISO"] = "PJMISO"
                df["AUCTION_NAME"] = auction_name
                df["AUCTION_TYPE"] = auction_type
                year = str(release_date.year)
                df["AUCTION_YEAR"] = year
                df['AUCTION_MONTH'] = None
                cols = list(df.columns)
                df = df[cols]
                df["AUCTION_ID"] = auction_df.iloc[0][1]
                df["AUCTION_ROUND"] = auction_df.iloc[0][2]
                df["RELEASE_DATE"] = str(release_date)
                df["INSERTED_DATE"] = str(datetime.now())
                if auction_type == "ANNUAL":
                    df["FLOWSTART_DATE"] = datetime.strptime(
                        "{}0601".format(year.split("-")[0]), "%Y%m%d")
                    df["FLOWEND_DATE"] = datetime.strptime(
                        "{}0531".format(year.split("-")[0]), "%Y%m%d")
                elif auction_type == "LONGTERM":
                    df["FLOWSTART_DATE"] = df["PERIOD_TYPE"].apply(lambda x: datetime.strptime(
                        "{}0601".format(str(int(x[-1]) - 1 + int(year.split("-")[0]))), "%Y%m%d"))
                    df["FLOWEND_DATE"] = df["FLOWSTART_DATE"].apply(
                        lambda x: datetime.strptime("{}0531".format(str(x.year + 1)), "%Y%m%d"))
                elif auction_type == "MONTHLY":
                    result_month = datetime.strptime(
                        sheet_name.split(' ')[0], '%b').month
                    df['AUCTION_MONTH'] = result_month
                    df['START_END_DATE'] = df['PERIOD_TYPE'].apply(
                        lambda x: calc_start_end_date_by_period(x, year, result_month))
                    df[['FLOWSTART_DATE', 'FLOWEND_DATE']] = pd.DataFrame(
                        df.START_END_DATE.tolist(), index=df.index)
                    df = df.drop(columns='START_END_DATE')
                df.rename(columns={'FLOWSTART_DATE': 'FLOW_STARTDATE',
                                    'FLOWEND_DATE': 'FLOW_ENDDATE', 'CONSTRAINT_NAME': 'CONSTRAINT'}, inplace=True)
                df = df[['BU_ISO','AUCTION_NAME','AUCTION_TYPE','AUCTION_YEAR',
                    'AUCTION_MONTH','CONSTRAINT','CTG_ID','PERIOD_TYPE','BU_PEAKTYPE','MARGINAL_VALUE',
                    'LIMIT','AUCTION_ID','AUCTION_ROUND','RELEASE_DATE','INSERTED_DATE','FLOW_STARTDATE','FLOW_ENDDATE']]

                df['RELEASE_DATE'] = df['RELEASE_DATE'].map(str)
                df['FLOW_ENDDATE'] = df['FLOW_ENDDATE'].map(str)
                df['FLOW_STARTDATE'] = df['FLOW_STARTDATE'].map(str)
                df['CTG_ID'] = df['CTG_ID'].map(str)
                df.drop_duplicates(subset=primary_key_list,
                                    keep='first', inplace=True)
        return df
                
    except Exception as e:
        logger.exception(e)
        raise e

def load_auction_constraint(auction_type):
    '''
        This function is responsible for loading of the data and formatting and uploading the final 
        output in the database. This function calls sub functions and does the whole thing

        Params:
            auction_type(str): The type of auction
    '''
    try:
        logger.info(f"The process started for {auction_type}")

        if auction_type == "ANNUAL":
            files = get_annual_ftr_auction()
        elif auction_type == "LONGTERM":
            files = get_longterm_ftr_auction()
        elif auction_type == "MONTHLY":
            files = get_monthly_ftr_auction()
        res = 0

        table = databasename + '.' + schemaname+'.' + tablename
        query_primary_key = f'''SHOW PRIMARY KEYS IN {table}'''
        # conn = get_connection()
        cursor = get_connection(database=databasename,schema=schemaname,role=role).cursor()
        cursor.execute(query_primary_key)
        result = cursor.fetchall()
        if len(result) > 0:
            for j in range(0, len(result)):
                primary_key_list.append(result[j][4].upper())
        logger.info(f"Primary keys for table are {primary_key_list}")
        
        csv_file = ''
        if auction_type == 'MONTHLY' and len(files) != 0:
            release_date = files[1]
            csv_file = files[0]
            xlsxfile = pd.ExcelFile(csv_file)
            logger.info(f"Format df function called for {auction_type}")
            df = format_df(csv_file, xlsxfile, release_date, auction_type)
            logger.info(f"Format df function completed for {auction_type}")
            if len(df) > 0:
                res += load_df_to_sf(
                    df, databasename, schemaname, tablename, primary_key_list, uploadinsert=False)
            else:
                return 0
        else:
            files.reverse() 
            for site, file, auction_name, release_date in files:
                try:
                    r = None
                    logger.info(site+file)
                    try:
                        for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(10)):
                            with attempt:
                                r = requests.get(site+file)
                                if r.status_code != 200:
                                    raise Exception
                    except RetryError:
                        break
                    csv_file = f"C:/Temp/pjm_constraints.xlsx"
                    with open(csv_file, "wb") as csv:
                        csv.write(r.content)
                    csv.close()
                    xlsxfile = pd.ExcelFile(csv_file)
                    logger.info(f"Format df function called for {auction_type}")
                    df = format_df(csv_file, xlsxfile, release_date, auction_type)
                    logger.info(f"Format df function completed for {auction_type}")
                    if len(df) > 0:
                        res += load_df_to_sf(
                            df, databasename, schemaname, tablename, primary_key_list, uploadinsert=False)
                    else:
                        return 0
                except Exception as e:
                    logger.exception(f'Error {e}')
                    raise e
        
        return res
    except Exception as e: 
        logger.exception(e)
        raise e   


def calc_start_end_date_by_period(period: str, year: str, result_month: int):
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

def load_df_to_sf(df, databasename, schemaname, tablename, primary_key_list, uploadinsert=False):
    try:
        logger.info("Inside load_df_to_sf function")
        DATABASE = databasename
        SCHEMA = schemaname   
        ROLE = f"OWNER_{databasename}"
        WAREHOURSE = "ITPYTHON_WH"
        engine = bu_snowflake.get_engine(
            warehouse=WAREHOURSE,
            role=ROLE,
            schema=SCHEMA,
            database=DATABASE
        )

        logger.info("Engine object created sccessfully")

        df.to_sql(tablename,
            con=engine,
            index=False,
            if_exists='append',
            schema = SCHEMA,
            method = functools.partial(pd_writer, quote_identifiers=False))

        logger.info(f"{len(df)} rows uploaded successfully")

        return len(df)

    except Exception as e:
        logger.exception(e)
        raise e
if __name__ == "__main__":
    
    sys.path.append(pathlib.Path().absolute().__str__())
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logfilename = bu_alerts.add_file_logging(logger,process_name='pjm_auction_constraint')
    logger.info(f'Execution started')
    files_location = os.getcwd() + "\\pjm_auction_constraint_download"

    try:
        credential_dict = get_config('PJMISO website data','PJMISO_AUCTION_CONSTRAINT')
        databasename = credential_dict['DATABASE']
        # databasename = "POWERDB_DEV"
        schemaname = credential_dict['TABLE_SCHEMA']
        tablename = credential_dict['TABLE_NAME']
        role = f"OWNER_{databasename}"
        receiver_email = credential_dict['EMAIL_LIST']
        username = credential_dict['USERNAME']
        password = credential_dict['PASSWORD']
        # receiver_email = "mrutunjaya.sahoo@biourja.com"
        print(databasename,schemaname,tablename)
        bu_alerts.bulog(
            process_name=tablename,
            status='started',
            table_name=tablename,
            process_owner=credential_dict['IT_OWNER'],
            database=databasename
        )
        logger.info("Calling remove_existing_files function")
        remove_existing_files(files_location)
        logger.info("Remove existing files completed successfully")

        auction_types = ["LONGTERM","ANNUAL","MONTHLY"]
        # auction_types = ["MONTHLY"]
        results = []
        res = 0
        for auction_type in auction_types:
            res += load_auction_constraint(auction_type)

            logger.info(f"Dataframe fetched  and uploaded successfully for {auction_type}")

       
        logger.info("Excutaion completed successfully")
        bu_alerts.bulog(
            process_name=tablename,
            status='Completed',
            table_name=tablename,
            process_owner=credential_dict['IT_OWNER'],
            database=databasename
        )
        bu_alerts.send_mail(
            receiver_email=receiver_email,
            mail_subject="JOB SUCCESS - {} rows uploaded {}".format(tablename,res),
            mail_body=f"{tablename} completed successfully, Attached logs",
            attachment_location=logfilename)
    except Exception as e:
        logger.exception(f'Error occured due to {e}')
        bu_alerts.bulog(
            process_name=tablename,
            status='Failed',
            table_name=tablename,
            process_owner=credential_dict['IT_OWNER'],
            database=databasename
        )
        bu_alerts.send_mail(
            receiver_email=receiver_email,
            mail_subject="JOB FAILED - {}".format(tablename),
            mail_body=f"{tablename} failed with error {e}, Attached logs",
            attachment_location=logfilename
        )
