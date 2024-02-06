import logging
import requests
import snowflake.connector
import csv
import os
import numpy as np 
import pandas as pd
from datetime import datetime
from bu_snowflake import get_connection
import bu_alerts

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

receiver_email = 'indiapowerit@biourja.com, DAPower@biourja.com'
# receiver_email = 'radha.waswani@biourja.com'

# log file location
log_file_location=os.getcwd() + '\\' + 'logs' + '\\' + 'PJM_AGG_DEF.txt'
if os.path.isfile(log_file_location):
    os.remove(log_file_location)

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] - %(message)s',
    filename=log_file_location)

# warehouse='ITPYTHON_WH'
tablename='POWERDB.ISO.PJM_AGG_DEF'
# tablename='POWERDB_DEV.ISO.PJM_AGG_DEF'

job_id=np.random.randint(1000000,9999999)

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
    
        conn=get_connection(role='OWNER_POWERDB',database='POWERDB',schema='ISO')
        cur=conn.cursor()
        cur.execute(sql)
        df_max_date=pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
        return df_max_date
    except Exception as e:
        print(f"Exception caught {e} in fetching max date for {auction_type}")
        logging.exception(f'Exception caught during execution: {e}')
        raise e
    else:
        cur.close()
        

def get_pjm_auctionwise_file(auction_type:str):
    """ This function will download file for PJM_B1B2B3_MAPPING TABLE
    based on auction type "MONTHLY" AND "ANNUAL"

    Args:
        auction_type (str): [description]

    Returns:
        [string or dataframe]: this will return dataframe if new record fetched from website
        
    """
    try:        
        # here downloaded file will store in this folder
        # extract_dir_name='download'
        # downloaded file name based on auction type
        file_name=f"PJM_AGG_DEF_{auction_type}.csv"
        # file location path 
        file_location = os.getcwd() + '\\' + 'download' + '\\' + file_name
        print(file_location)

        # based on auction_type will select different url
        if auction_type=='ANNUAL':
            url="http://www.pjm.com/pub/account/auction-user-info/model-annual/Annual-aggregate-def.csv"
        else:
            url="http://www.pjm.com/pub/account/auction-user-info/model-monthly/aggregate-def.csv"
            

        file_data=requests.get(url)
        # if file exist than first delete
        if os.path.isfile(file_location):
            os.remove(file_location)
        # writing the file content to local folder file 
        with open(file_location,'wb') as file_name:
            file_name.write(file_data.content)
        # reading the posted_date from the downloaded file 
        posted_date=pd.read_csv(file_location)
        posted_date = posted_date.columns[0][9:]
        posted_date=datetime.strptime(posted_date,'%Y%m%d').strftime('%Y-%m-%d')
        print("Posted Date ::::::::",posted_date)
        # get max release  date from the "powerdb" database 
        max_release_date=get_max_release_date(auction_type)
        if max_release_date.empty:
            print(f"release date is not thre for {auction_type} type")
            max_release_date_db = ''
        else:
            max_release_date=max_release_date['RELEASE_DATE'][0]
            max_release_date_db= max_release_date.strftime('%Y-%m-%d')
            print("Max Release Date : {}".format(max_release_date_db))
        
        
        if max_release_date_db==posted_date:         
            # process abandon if database date and posted date are equal
            return f"no_new_records for {auction_type} auction"

        auction_new_record=pd.read_csv(file_location,header=1)
        # adding columns to dataframe
        auction_new_record=auction_new_record.reindex(columns=["Name","Type","B1","B2","B3","Bus #","Factor","AUCTION_TYPE","MODEL_IDX","RELEASE_DATE",'INSERTED_DATE'])
        auction_new_record['RELEASE_DATE']=posted_date
        auction_new_record['AUCTION_TYPE']=auction_type
        auction_new_record['BU_ISO']='PJMISO'
        auction_new_record['MODEL_IDX']=0
       
        #adding insert_date column 
        auction_new_record['INSERTED_DATE']=datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        auction_new_record.rename(columns={'Bus #':'BUS_ID','Factor':'FACTOR'}, inplace=True)
        #print(auction_new_record.columns)
        auction_new_record=auction_new_record[["BU_ISO","Name","Type","B1","B2","B3","BUS_ID","FACTOR","AUCTION_TYPE","MODEL_IDX","RELEASE_DATE",'INSERTED_DATE']]
        
        # csv name it will upload to sf
        snow_upload_csv=f"SNOW_PJM_AGG_DEF_{auction_type}.csv"
        # snow_upload_csv_location = os.getcwd() + '\\' + extract_dir_name + '\\'+snow_upload_csv
        # auction_new_record.to_csv(snow_upload_csv, index=False, date_format='%Y-%m-%d', quoting=csv.QUOTE_MINIMAL)
        auction_new_record.to_csv(snow_upload_csv, index=False,  quoting=csv.QUOTE_MINIMAL)
        conn=get_connection(role='OWNER_POWERDB',database='POWERDB',schema='ISO')
        # starting a transaction if any error will occur in this try block than rollback all the dml operations
        conn.cursor().execute('BEGIN')

        # removing earlier stage file from staging area
        conn.cursor().execute('remove @%PJM_AGG_DEF')
        
        conn.cursor().execute("PUT file://{} @%PJM_AGG_DEF overwrite=true".format(snow_upload_csv))
        conn.cursor().execute('''
                COPY INTO PJM_AGG_DEF file_format=(type=csv 
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
        return auction_new_record
    except Exception as e:
        print(f'Exception caught during execution: {e}')
        logging.exception(f'Exception caught during execution: {e}')
        conn.cursor().execute('rollback')           
    else:
        conn.cursor().execute('commit')
        conn.close()         
            

if __name__ == "__main__": 
    starttime=datetime.now()
    logging.info('Execution Started')
    logging.info('Start work at {} ...'.format(starttime.strftime('%Y-%m-%d %H:%M:%S')))
    rows=0
    try:
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name="PJM_AGG_DEF",database='POWERDB',status='Started',table_name=tablename, row_count=rows, log=log_json, warehouse='ITPYTHON_WH',process_owner='radha/rahul')
        # getting both auction file "MONTHLY" AND "ANNUAL"
        auction_type_list=['ANNUAL','MONTHLY']
        for auction_type in auction_type_list:
            result=get_pjm_auctionwise_file(auction_type)
            # here we check above result variable if it is dataframe it means new records fetched than print number of records
            if isinstance(result, pd.DataFrame):
                rows = rows + len(result)
                print(f"new rows inserted for {auction_type}  {len(result)}")
            else:
                print(result)
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name="PJM_AGG_DEF",database='POWERDB',status='Completed',table_name=tablename, row_count=rows, log=log_json, warehouse='ITPYTHON_WH',process_owner='radha/rahul') 
        logging.info('Execution Done')
        bu_alerts.send_mail(
            receiver_email = receiver_email,
            mail_subject ='JOB SUCCESS - PJM_AGG_DEF',
            mail_body = 'PJM_AGG_DEF completed successfully, Attached logs',
            attachment_location = log_file_location
        )
    except Exception as e:
        print("Exception caught during execution: ",e)
        logging.exception(f'Exception caught during execution: {e}')
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name="PJM_AGG_DEF",database='POWERDB',status='Failed',table_name= tablename, row_count=0, log=log_json, warehouse='ITPYTHON_WH',process_owner='radha/rahul') 
        bu_alerts.send_mail(
            receiver_email = receiver_email,
            mail_subject='JOB FAILED - PJM_AGG_DEF',
            mail_body='PJM_AGG_DEF failed during execution, Attached logs',
            attachment_location = log_file_location
        )

    endtime=datetime.now()
    print('Complete work at {} ...'.format(endtime.strftime('%Y-%m-%d %H:%M:%S')))
    print('Total time taken: {} seconds'.format((endtime-starttime).total_seconds()))