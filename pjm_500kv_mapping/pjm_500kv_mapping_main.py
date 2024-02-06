import logging
import requests
import os
import pandas as pd
import numpy as np
import bu_alerts
from datetime import datetime
from snowflake.connector.pandas_tools import write_pandas
from bu_snowflake import get_connection
# To get credentials used in process
from bu_config import get_config
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
import os
import time


for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# log file location
log_file_location = os.getcwd() + '\\' + 'logs' + '\\' + 'PJM_500KV_LOG.txt'
if os.path.isfile(log_file_location):
    os.remove(log_file_location)

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] - %(message)s',
    filename=log_file_location)

# tablename='POWERDB_DEV.ISO.PJM_500KV_MAPPING'

job_id=np.random.randint(1000000,9999999)

def remove_existing_files(files_location):
    logging.info("Inside remove_existing_files function")
    try:
        files = os.listdir(files_location)
        if len(files) > 0:
            for file in files:
                os.remove(files_location + "\\" + file)

            logging.info("Existing files removed successfully")
        else:
            print("No existing files available to reomve")
        print("Pause")
    except Exception as e:
        logging.info(e)
        raise e

def get_max_release_date(auction_type:str):
    """  Here we get max release date for particular auction type

    Args:
        auction_type (str): will be MONTHLY or ANNUAL

    Returns:
        dataframe : will return a dataframe
    """
    try:
        sql=f"""
                select max(RELEASE_DATE) AS RELEASE_DATE,
                    AUCTION_TYPE
                from {tablename}
                where AUCTION_TYPE = '{auction_type}'
                group by AUCTION_TYPE
            """
        conn=get_connection(role=f'OWNER_{databasename}',database=databasename,schema='ISO')
        cur=conn.cursor()
        cur.execute(sql)
        df_max_date=pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
        return df_max_date
    except Exception as e:
        print(f"Exception caught {e} in fetching max date for {auction_type}")
        logging.exception(f'Exception caught during execution: {e}')
        raise e


def get_pjm_auctionwise_file(auction_type:str):
    """ This function will download file for PJM_500KV_MAPPING TABLE
    based on auction type "MONTHLY" AND "ANNUAL"

    Args:
        auction_type (str): [description]

    Returns:
        [string or dataframe]: this will return dataframe if new record fetched from website
        
    """
    try:      
        # based on auction_type will select different url
        if auction_type=='ANNUAL':
            url=(credential_dict['SOURCE_URL'].split(';')[0])
        else:
            url=(credential_dict['SOURCE_URL'].split(';')[1])            

        files_location = os.getcwd() + "\\pjm_500kv_mapping\\download_pjm_500kv"
        file_name = url.split('/')[-1]
        logging.info(f"File name extracted: {file_name}")

        logging.info("Calling remove_existing_files function")
        remove_existing_files(files_location)
        logging.info("Remove existing files completed successfully")

        options = Options()
        options.set_preference("browser.download.folderList", 2)
        options.set_preference("browser.download.manager.showWhenStarting", False)
        options.set_preference("browser.download.dir", files_location)
        browser = webdriver.Firefox(options=options, executable_path='geckodriver.exe')
        logging.info(f"Browser object created")
        browser.get(url)
        time.sleep(30)
        browser.find_element(By.ID,"idToken1").send_keys(credential_dict['USERNAME'])
        browser.find_element(By.ID,"idToken2").send_keys(credential_dict['PASSWORD'])
        browser.find_element(By.ID,"loginButton_0").click()
        time.sleep(20)
        logging.info(f"File download from browser")
        browser.quit()
        logging.info("Browser object quit successfully")

        if len(os.listdir(files_location)) == 0:
            raise Exception("File could not be downloaded from browser due to website issue")

        # reading the posted_date from the downloaded file 
        posted_date=pd.read_csv(files_location + '\\' + file_name,nrows=0)
        posted_date = posted_date.columns[0][9:]
        posted_date=datetime.strptime(posted_date,'%Y%m%d').strftime('%Y-%m-%d')
        # get max release  date from the "powerdb" database 
        max_release_date=get_max_release_date(auction_type)
        max_release_date=max_release_date['RELEASE_DATE'][0]
        max_release_date_db= max_release_date.strftime('%Y-%m-%d')

        auction_new_record=pd.read_csv(files_location + '\\' + file_name,header=1)

        os.remove(files_location + '\\' + file_name)
        logging.info(f"File removed after reading")
        
        if max_release_date_db==posted_date:
            # process abandon if database date and posted date are equal            
            return 0

        # adding columns to dataframe
        auction_new_record['RELEASE_DATE']=posted_date
        auction_new_record['AUCTION_TYPE']=auction_type
        auction_new_record['BU_ISO']='PJMISO'
        auction_new_record['MODEL_IDX']=0
        # renaming columns
        auction_new_record.rename(columns={'Valid Source/Sink List':'SOURCE_SINK','Type':'TYPE','Bus #':'BUS'}, inplace=True)
        #adding insert_date column 
        auction_new_record['INSERT_DATE']=datetime.now().strftime("%m/%d/%Y %H:%M:%S")

        auction_new_record=auction_new_record[['BU_ISO','SOURCE_SINK','B1-B2-B3','TYPE','BUS','AUCTION_TYPE','MODEL_IDX','RELEASE_DATE','INSERT_DATE']]
        conn=get_connection(role=f'OWNER_{databasename}',database=databasename,schema='ISO')
        
        succes,nchunks,nrows,_ = write_pandas(
            conn=conn,
            df= auction_new_record,
            database=credential_dict['DATABASE'],
            schema=credential_dict['TABLE_SCHEMA'],
            table_name=credential_dict['TABLE_NAME'],
            # quote_identifiers=False
        )
        print(f'Added {nrows} in {nchunks} chunks to table - {succes}. Now updating model index')
        
        # this query will update model_idx column in such a way earlier records of model_idx subtracted by -1
        conn.cursor().execute(f"""
                                UPDATE {tablename} pd
                                SET MODEL_IDX = ((ps.row_number * -1) + 1)
                                from (
                                        select dense_rank() over (
                                                order by RELEASE_DATE desc
                                            ) as row_number,
                                            RELEASE_DATE
                                        from {tablename}
                                        where AUCTION_TYPE = '{auction_type}'
                                        group by RELEASE_DATE
                                    ) ps
                                WHERE pd.RELEASE_DATE = ps.RELEASE_DATE
                                    and pd.AUCTION_TYPE = '{auction_type}'
                                """)
        return nrows
    except Exception as e:     
        print(f'Exception caught during execution: {e}')
        logging.exception(f"The file couldn't be downloaded from website due to website issue")
        logging.exception(f'Exception caught during execution: {e}')
        try:
            conn.cursor().execute('rollback') 
        except Exception as ex:
            logging.exception(f"No connection object found {ex}")   

        raise e
    
if __name__ == "__main__": 
    logging.info('Execution Started')
    starttime=datetime.now()
    logging.info('Start work at {} ...'.format(starttime.strftime('%Y-%m-%d %H:%M:%S')))
    rows=0
    try:
        credential_dict = get_config('PJMISO website data','PJM_500KV_MAPPING')
        databasename = credential_dict['DATABASE']
        # databasename = "POWERDB_DEV"
        tablename = databasename+'.'+credential_dict['TABLE_SCHEMA']+'.'+credential_dict['TABLE_NAME']
        print(tablename)
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name=credential_dict['TABLE_NAME'],database=databasename,
                        status='Started',table_name=tablename, 
                        row_count=rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=credential_dict['IT_OWNER'])

        auction_type_list=['ANNUAL','MONTHLY']
    
        for auction_type in auction_type_list:
            rows=get_pjm_auctionwise_file(auction_type)
            # here we check above result variable if it is dataframe it means new records fetched than print number of records
            if rows != 0:
                print(f"new rows inserted for {auction_type}  {rows}")
            else:
                print(f'nothing new to insert have {rows} rows')
        
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name=credential_dict['TABLE_NAME'],database=databasename,
                        status='Completed',table_name=tablename, row_count=rows, log=log_json,
                        warehouse='ITPYTHON_WH',process_owner=credential_dict['IT_OWNER']) 
        logging.info('Execution Done')
        bu_alerts.send_mail(
            receiver_email = credential_dict['EMAIL_LIST'],
            mail_subject ='JOB SUCCESS - {}'.format(credential_dict['TABLE_NAME']),
            mail_body = '{} completed successfully, Attached logs'.format(credential_dict['TABLE_NAME']),
            attachment_location = log_file_location
        )
    except Exception as e:
        print("Exception caught during execution: ",e)
        logging.exception(f'Exception caught during execution: {e}')
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name=credential_dict['TABLE_NAME'],database=databasename,
                        status='Failed',table_name= tablename, row_count=rows, log=log_json, \
                        warehouse='ITPYTHON_WH',process_owner=credential_dict['IT_OWNER']) 
        bu_alerts.send_mail(
            receiver_email = credential_dict['EMAIL_LIST'],
            mail_subject='JOB FAILED - {}  {}'.format(credential_dict['TABLE_NAME'],e),
            mail_body='{} failed during execution, Attached logs'.format(credential_dict['TABLE_NAME']),
            attachment_location = log_file_location
        )

    endtime=datetime.now()
    print('Complete work at {} ...'.format(endtime.strftime('%Y-%m-%d %H:%M:%S')))
    print('Total time taken: {} seconds'.format((endtime-starttime).total_seconds()))