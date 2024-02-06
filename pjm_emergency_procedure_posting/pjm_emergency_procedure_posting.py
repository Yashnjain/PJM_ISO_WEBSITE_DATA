import numpy as np
import pandas as pd
import os
import xml.etree.ElementTree as ET
from datetime import datetime,timedelta
from functools import reduce
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
import time
import logging
import bu_alerts
import bu_snowflake
import functools
from snowflake.connector.pandas_tools import pd_writer
import bu_config

def convert_date(s):
    if s is not None:
        return str(datetime.strptime(s,'%Y-%m-%dT%H:%MZ'))
    else:
        return s
    

def remove_existing_files(download_location):
    logger.info("Inside remove_existing_files function")
    try:
        files = os.listdir(download_location)
        if len(files) > 0:
            for file in files:
                os.remove(download_location + "\\" + file)

            logger.info("Existing files removed successfully")
        else:
            print("No existing files available to reomve")
        print("Pause")
    except Exception as e:
        logger.info(e)
        raise e

def download_data(url,download_location,executable_path):
    try:
        print("Inside download_data function")
        logger.info("Inside download_data function")
        options = Options()
        logger.info("Options object created")
        mime_types = ['application/pdf', 'text/plain', 'application/vnd.ms-excel',
            'text/csv', 'application/csv', 'text/comma-separated-values',
            'application/download', 'application/octet-stream', 'binary/octet-stream',
            'application/binary', 'application/x-unknown','attachment/csv',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']
        options.set_preference("browser.download.folderList", 2)
        options.set_preference("browser.download.manager.showWhenStarting", False)
        options.set_preference("browser.download.dir", download_location)
        options.set_preference("browser.helperApps.neverAsk.saveToDisk",",".join(mime_types))
        options.set_preference("browser.helperApps.neverAsk.openFile", """application/pdf,
            application/octet-stream, application/x-winzip, application/x-pdf, application/x-gzip""")
        options.set_preference("pdfjs.disabled", True)
        # service = Service(executable_path)
        logger.info("Executable path for geckodriver passed successfully")

        # browser = webdriver.Firefox(service = service, options=options)
        browser = webdriver.Firefox(options=options, executable_path=executable_path)
        browser.get(url)
        time.sleep(5)
        logger.info("Browser get successfully")
        browser.maximize_window()
        print(browser.title)

        xml_xpath = browser.find_element(By.ID,"frmButtons:lnkDownload")
        xml_xpath.click()
        time.sleep(10)
        logger.info("XML file downloaded")
    except Exception as e:
        logger.exception(e)
        raise e
    finally:      
        browser.quit()
        logger.info("Browser quit successfully")

def get_tag_list(val):
    logger.info("Inside get_tag_list function")
    try:
        tag_list = []
        reg_tag = []
        msg_id = msg_type = pstd_time = cancl_time = msg = priority = eff_st_time = eff_end_time = None
        elems = list(val)
        for e in elems:
            if e.tag == "messageId":
                msg_id = e.text
            elif e.tag == "messageType":
                msg_type = e.text
            elif e.tag == "postedTimestamp":
                pstd_time = e.text
            elif e.tag == "canceledTimestamp":
                cancl_time = e.text
            elif e.tag == "message":
                msg = e.text
            elif e.tag == "priority":
                priority = e.text
            elif e.tag == "effectiveStartTime":
                eff_st_time = e.text
            elif e.tag == "effectiveEndTime":
                eff_end_time = e.text
            elif e.tag == "Region":
                reg = list(e)
                reg_tag.append([r.text  for r in reg if r.tag == "regionName"][0])

        tag_list.append({"MESSAGE_ID":msg_id or None,"PRIORITY":priority or None,"MESSAGE_TYPE":msg_type or None, 
        "EMERGENCY_MESSAGE":msg or None,"EFFECTIVE_START_TIME_UTC":eff_st_time or None,"EFFECTIVE_END_TIME_UTC":eff_end_time or None,
        "POSTING_TIME_UTC":pstd_time,"CANCELLATION_TIME_UTC":cancl_time,"REGIONS": ",".join(reg_tag)})

        logger.info("Tag list created")
        print(tag_list)

        return tag_list
    except Exception as e:
        logger.exception(e)
        print(e)
        raise e

def extract_xml(download_location):
    logger.info("Inside extract_xml function")
    data_list = []
    try:
        for _,_,files in os.walk(download_location):
            for file in files:
                logger.info(f"Extraction started for file {file}")
                root = ET.parse(download_location + "\\" + file).getroot()
                logger.info("Root object created")
                children = list(root)
                for child in children:
                    if child.tag == "EmergencyMessage":
                        val = list(child)
                        data_list.append(get_tag_list(val))
                    
        if len(data_list) != 0:
            final_list = reduce(lambda x,y: x + y, data_list)
            main_df = pd.DataFrame(final_list)
            logger.info("Main dataframe created")
            main_df.EFFECTIVE_START_TIME_UTC = main_df.EFFECTIVE_START_TIME_UTC.apply(convert_date)
            main_df.EFFECTIVE_END_TIME_UTC = main_df.EFFECTIVE_END_TIME_UTC.apply(convert_date)
            main_df.POSTING_TIME_UTC = main_df.POSTING_TIME_UTC.apply(convert_date)
            main_df.CANCELLATION_TIME_UTC = main_df.CANCELLATION_TIME_UTC.apply(convert_date)
            logger.info("All the datetime columns converted")
            main_df["INSERT_DATE"] = str(datetime.now())
            main_df["UPDATE_DATE"] = str(datetime.now())
            logger.info("Insert and Update date added in main dataframe")
            print(main_df)
            logger.info("Returning main_df")

            return main_df
        else:
            logger.info("No new data found in the website so data list can't be created")
            logger.info("Returing None")

            return
    except Exception as e:
        logger.exception(e)
        print(e)
        raise e


def upload_in_sf(tablename, df):
    logger.info("Inside upload_in_sf function")
    try:
        engine = bu_snowflake.get_engine(
                    database=databasename,
                    role=f"OWNER_{databasename}",    
                    schema= schemaname                           
                )
        with engine.connect() as conn:
            logger.info("Engine object created successfully")

            temp_tablename = f"TEMP_{tablename}"
            temp_table_query = f"""
                            create or replace temporary TABLE {databasename}.PTEMP.{temp_tablename} (
                            MESSAGE_ID number(18,0),
                            PRIORITY  varchar(20),
                            MESSAGE_TYPE varchar(150),
                            REGIONS varchar(500),
                            EMERGENCY_MESSAGE varchar(1700),
                            EFFECTIVE_START_TIME_UTC datetime,
                            EFFECTIVE_END_TIME_UTC datetime,
                            POSTING_TIME_UTC datetime,
                            CANCELLATION_TIME_UTC datetime,
                            INSERT_DATE datetime,
                            UPDATE_DATE datetime,
                            primary key(MESSAGE_ID)
                        );
                """
            conn.execute(temp_table_query)
            logger.info("Temporary table created successfully")
            df.to_sql(temp_tablename.lower(), 
                    con=conn,
                    index=False,
                    if_exists='append',
                    schema="PTEMP",
                    method=functools.partial(pd_writer, quote_identifiers=False)
                    )

            merge_query = f'''merge into {databasename}.{schemaname}.{tablename} t using {databasename}.PTEMP.{temp_tablename} s 
                            on t.MESSAGE_ID = s.MESSAGE_ID
                            when matched then
                            update
                            set 
                                t.MESSAGE_ID = s.MESSAGE_ID,
                                t.PRIORITY = s.PRIORITY,
                                t.MESSAGE_TYPE = s.MESSAGE_TYPE,
                                t.REGIONS = s.REGIONS,
                                t.EMERGENCY_MESSAGE = s.EMERGENCY_MESSAGE,
                                t.EFFECTIVE_START_TIME_UTC = s.EFFECTIVE_START_TIME_UTC,
                                t.EFFECTIVE_END_TIME_UTC = s.EFFECTIVE_END_TIME_UTC,
                                t.POSTING_TIME_UTC = s.POSTING_TIME_UTC,
                                t.CANCELLATION_TIME_UTC = s.CANCELLATION_TIME_UTC,
                                t.UPDATE_DATE = s.UPDATE_DATE
                                when not matched then
                            insert (
                                    MESSAGE_ID,
                                    PRIORITY,
                                    MESSAGE_TYPE,
                                    REGIONS,
                                    EMERGENCY_MESSAGE,
                                    EFFECTIVE_START_TIME_UTC,
                                    EFFECTIVE_END_TIME_UTC,
                                    POSTING_TIME_UTC,
                                    CANCELLATION_TIME_UTC,
                                    INSERT_DATE,
                                    UPDATE_DATE
                                )
                            values (
                                    s.MESSAGE_ID,
                                    s.PRIORITY,
                                    s.MESSAGE_TYPE,
                                    s.REGIONS,
                                    s.EMERGENCY_MESSAGE,
                                    s.EFFECTIVE_START_TIME_UTC,
                                    s.EFFECTIVE_END_TIME_UTC,
                                    s.POSTING_TIME_UTC,
                                    s.CANCELLATION_TIME_UTC,
                                    s.INSERT_DATE,
                                    s.UPDATE_DATE
                                )'''
            res = conn.execute(merge_query).fetchall()[0][0]

            logger.info(f"{res} number of rows uploaded")
                
        return res
    except Exception as e:
        logger.exception("Exception while inserting data into snowflake")
        logger.exception(e)
        raise e
    

if __name__ == "__main__":    
    try:
        job_id=np.random.randint(1000000,9999999)
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        logfilename = bu_alerts.add_file_logging(logger,process_name= 'PJM_EMERGENCY_PROCEDURE_POSTING')

        logger.info("Execution started")    
        credential_dict = bu_config.get_config('PJMISO website data','PJM_EMERGENCY_PROCEDURE_POSTING')
        processname = credential_dict['PROJECT_NAME']
        databasename = credential_dict['DATABASE']
        # databasename = 'POWERDB_DEV'
        schemaname = credential_dict['TABLE_SCHEMA']
        tablename = credential_dict['TABLE_NAME']
        url = credential_dict['SOURCE_URL']
        # url = "https://emergencyprocedures.pjm.com/ep/pages/dashboard.jsf"
        process_owner = credential_dict['IT_OWNER']
        receiver_email = credential_dict['EMAIL_LIST']
        # receiver_email = "Mrutunjaya.Sahoo@biourja.com,radha.waswani@biourja.com"
        # receiver_email = "priyanka.solanki@biourja.com"

        logger.info("All the credential details fetched from creential dict")

        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'

        bu_alerts.bulog(process_name=processname,database=databasename,status='Started',table_name=tablename,
            row_count=0, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)

        download_location = os.getcwd() + "\\pjm_emergency_procedure_posting\\download"
        logger.info("Download location created")

        executable_path = os.getcwd() + "\\pjm_emergency_procedure_posting\\geckodriver.exe"

        logger.info("Executable path created")

        logger.info("Calling remove files function")
        remove_existing_files(download_location)
        logger.info("Remove files function completed")

        logger.info("Calling download data function")
        download_data(url,download_location,executable_path)
        logger.info("Download data function completed")

        logger.info("Calling extract xml function")
        df = extract_xml(download_location)
        logger.info("Extract xml function completed")

        if df is not None:
            logger.info("Calling upload to sf function")
            rows = upload_in_sf(tablename, df)
            logger.info("Upload to sf function completed")
        else:
            logger.info("No data found in the website so not calling upload to sf function")
            rows = 0
        
        bu_alerts.bulog(process_name=processname,database=databasename,status='Completed',table_name=tablename,
            row_count=rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)
        if rows == 0:
            subject = f"JOB SUCCESS - {tablename}  NO new data found"
        else:
            subject = f"JOB SUCCESS - {tablename}  inserted {rows} rows"

        bu_alerts.send_mail(
            receiver_email = receiver_email, 
            mail_subject = subject,
            mail_body=f'{tablename} completed successfully, Attached logs',
            attachment_location = logfilename
        )
    except Exception as e:
        print("Exception caught during execution: ",e)
        logging.exception(f'Exception caught during execution: {e}')
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name= processname,database=databasename,status='Failed',table_name=tablename,
            row_count = 0, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)

        bu_alerts.send_mail(
            receiver_email = receiver_email,
            mail_subject = f'JOB FAILED - {tablename}',
            mail_body=f'{tablename} failed during execution, Attached logs',
            attachment_location = logfilename
        )