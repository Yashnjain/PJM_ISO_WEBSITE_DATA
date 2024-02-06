# -*- coding: utf-8 -*-
"""
Created on Fri Jul 24 18:02:15 2020

@author: Manish.Gupta
@description: this script will be fetch the data for PJM_BRANCH_MAPPING table from pjm api and push into snowflake database
"""
import logging
import requests
import pandas as pd
import csv
import os
import bu_alerts
from datetime import datetime, date
from bu_snowflake import get_connection
import numpy as np
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
log_file_location = os.getcwd() + '\\' + 'logs' + '\\' + 'PJM_BRANCH_MAPPING.txt'
if os.path.isfile(log_file_location):
    os.remove(log_file_location)

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] - %(message)s',
    filename=log_file_location)

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
        sql=f'''select max(RELEASE_DATE) AS RELEASE_DATE,AUCTION_TYPE from {tablename}
                               where AUCTION_TYPE='{auction_type}'
                               group by AUCTION_TYPE  '''
        conn=get_connection(role=f'OWNER_{databasename}',database=databasename,schema='ISO')
        cur=conn.cursor()
        cur.execute(sql)
        df_max_date=pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
        print(df_max_date)
        return df_max_date
    except Exception as e:
        print(f"Exception caught {e} in fetching max date for {auction_type}")
        logging.exception(f'Exception caught during execution: {e}')
        cur.close()
        raise e
        # return None
    else:
        cur.close()
        

#Get the pjm_branch_mapping file
def get_df_pjm_branch_mapping(auction_type: str):
    """ This function will download file for PJM_BRANCH_MAPPING TABLE
    based on auction type "MONTHLY" AND "ANNUAL"

    Args:
        auction_type (str): [description]

    Returns:
        [string or dataframe]: this will return dataframe if new record fetched from website
        
    """
    try:
        # based on auction_type will select different url
        if auction_type=='ANNUAL':
            # url="http://www.pjm.com/pub/account/auction-user-info/model-annual/Annual-PSSE-Branch-Mapping-File.csv"
            url = credential_dict['SOURCE_URL'].split(';')[0]
        else:
            # url="http://www.pjm.com/pub/account/auction-user-info/model-monthly/PSSE-Branch-Mapping-File.csv"
            url = credential_dict['SOURCE_URL'].split(';')[1]
       
        files_location = os.getcwd() + "\\pjm_branch_mapping\\download_branch"
        file_name = url.split('/')[-1]

        logging.info("Calling remove_existing_files function")
        remove_existing_files(files_location)
        logging.info("Remove existing files completed successfully")

        logging.info(f"File name extracted: {file_name}")
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
        time.sleep(50)
        logging.info(f"File download from browser")
        browser.quit()
        logging.info("Browser object quit successfully")

        if len(os.listdir(files_location)) == 0:
            raise Exception("File could not be downloaded from browser due to website issue")

        posted_date=pd.read_csv(files_location + '\\' + file_name,nrows=0)
        posted_date = posted_date.columns[0][9:]
        posted_date=datetime.strptime(posted_date,'%Y%m%d').strftime('%Y-%m-%d')
        print("Posted Date : {}".format(posted_date))  
    except Exception as e:
        print(f"Exception caught {e} while fetching data for {auction_type} type")
        logging.exception(f'Exception caught during execution: {e}')
        raise e
        # return f"got error {e} while fetching data for {auction_type} type"
    else:
        try:
            # get max release  date from the "powerdb" database 
            max_release_date=get_max_release_date(auction_type)
            print("Check max release date dataframe is avilable ")
            if max_release_date.empty:
                print(f"release date is not thre for {auction_type} type")
                max_release_date_db = ''
            else:
                max_release_date=max_release_date['RELEASE_DATE'][0]
                max_release_date_db= max_release_date.strftime('%Y-%m-%d')
                print("Max Release Date : {}".format(max_release_date_db))

            #read the downloaded csv file 
            print("read the downloaded csv file")
            new_auction_data = pd.read_csv(files_location + '\\' + file_name, header=2)

            os.remove(files_location + '\\' + file_name)
            logging.info(f"File removed after reading")
            
            if max_release_date_db==posted_date:
                # process abandon if database date and posted date are equal
                return f"no_new_records for {auction_type} auction"
            
           
            list_of_columns = ["BRANCH_ID_1","BRANCH_ID_2","BRANCH_ID_3","FROM_BUS","TO_BUS", "CIRCUIT_ID"]
            new_auction_data.columns = list_of_columns
            updated_auction_data = pd.DataFrame(new_auction_data, columns=list_of_columns)
            new_auction_data['RELEASE_DATE']=posted_date
            new_auction_data['AUCTION_TYPE']=auction_type
            new_auction_data['BU_ISO']='PJMISO'
            new_auction_data['MODEL_IDX']=0
            new_auction_data['INSERTED_DATE']=date.today()
            print(list(new_auction_data.columns))
            updated_auction_data = new_auction_data[["BU_ISO","BRANCH_ID_1","BRANCH_ID_2","BRANCH_ID_3","FROM_BUS","TO_BUS", "CIRCUIT_ID", "RELEASE_DATE","AUCTION_TYPE","MODEL_IDX","INSERTED_DATE"]]
            print(updated_auction_data)
        
            sf_upload_csv=f"SF_PJM_BRANCH_MAPPING_{auction_type}.csv"
            #sf_upload_csv_location = os.getcwd() + '\\' + extract_dir_name + '\\'+sf_upload_csv
            sf_upload_csv_location = sf_upload_csv
            print(sf_upload_csv_location)
            updated_auction_data.to_csv(sf_upload_csv_location, index=False, date_format='%Y-%m-%d', quoting=csv.QUOTE_MINIMAL)
            #get snowflake connection
            conn=get_connection(role=f'OWNER_{databasename}',database=databasename,schema='ISO')
            # removing earlier stage file from staging area
            conn.cursor().execute('remove @%PJM_BRANCH_MAPPING')
                
            conn.cursor().execute("PUT file://{} @%PJM_BRANCH_MAPPING overwrite=true".format(sf_upload_csv_location))
            conn.cursor().execute('''
                   COPY INTO PJM_BRANCH_MAPPING file_format=(type=csv 
                    skip_header=1 field_optionally_enclosed_by = '"' empty_field_as_null=true)
                    ''')
            # this query will update model_idx column in such a way earlier records of model_idx subtracted by -1
            conn.cursor().execute(f''' UPDATE {tablename} pd 
            SET MODEL_IDX = ((ps.row_number * -1) +1)
                    from (
                    select dense_rank()
                    over (order by RELEASE_DATE desc) as row_number, RELEASE_DATE from  {tablename} where AUCTION_TYPE = '{auction_type}'
                    group by RELEASE_DATE
                    )ps
                    WHERE pd.RELEASE_DATE= ps.RELEASE_DATE and 
                    pd.AUCTION_TYPE = '{auction_type}' ''')         
        except Exception as e:
            print("Exception caught during execution:",e)
            logging.exception(f'Exception caught during execution: {e}')
            conn.cursor().execute('rollback')           
            conn.close()
            raise e
        else:
            conn.cursor().execute('commit')
            conn.close()
        return updated_auction_data
    return f'NO NEW RECORD FOR THIS {auction_type} TYPE'
       
    
if __name__ == "__main__":
    starttime=datetime.now()
    logging.info('Execution Started')
    rows=0
    try:
        credential_dict = get_config('PJMISO website data','PJM_BRANCH_MAPPING')
        databasename = credential_dict['DATABASE']
        # databasename = "POWERDB_DEV"
        schemaname = credential_dict['TABLE_SCHEMA']
        tablename = credential_dict['TABLE_NAME']
        print(tablename)
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name=tablename,database=databasename,status='Started',
                        table_name=tablename, row_count=rows, log=log_json, warehouse='ITPYTHON_WH',
                        process_owner=credential_dict['IT_OWNER'])
        auction_type_list=['ANNUAL','MONTHLY']
        for auction_type in auction_type_list:
            result=get_df_pjm_branch_mapping(auction_type)
            # here we check above result variable if it is dataframe it means new records fetched than print number of records
            if isinstance(result, pd.DataFrame):
                rows = rows + len(result)
                print(f"new rows inserted for {auction_type}  {len(result)}")
            else:
                print(result)
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name="PJM_BRANCH_MAPPING",database=databasename,status='Completed',
                        table_name=tablename, row_count=rows, log=log_json, warehouse='ITPYTHON_WH',
                        process_owner=credential_dict['IT_OWNER']) 
        logging.info('Execution Done')
        bu_alerts.send_mail(
            # receiver_email = 'radha.waswani@biourja.com',
            receiver_email = credential_dict['EMAIL_LIST'],
            mail_subject ='JOB SUCCESS - {}'.format(tablename),
            mail_body = '{} completed successfully, Attached logs'.format(tablename),
            attachment_location = log_file_location
        )
    except Exception as e:
        print("Exception caught during execution: ",e)
        logging.exception(f'Exception caught during execution: {e}')
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name=tablename,database=databasename,status='Failed',
                        table_name= tablename, row_count=rows, log=log_json, 
                        warehouse='ITPYTHON_WH',process_owner=credential_dict['IT_OWNER']) 
        bu_alerts.send_mail(
            # receiver_email = 'radha.waswani@biourja.com',
            receiver_email = credential_dict['EMAIL_LIST'],
            mail_subject='JOB FAILED - {}  {}'.format(tablename,e),
            mail_body='{} failed during execution, Attached logs'.format(tablename),
            attachment_location = log_file_location
        )
    endtime=datetime.now()
    print('Complete work at {} ...'.format(endtime.strftime('%Y-%m-%d %H:%M:%S')))
    print('Total time taken: {} seconds'.format((endtime-starttime).total_seconds()))            
    
