import logging
import os 
import requests
import json
import csv 
import pandas as pd
from datetime import datetime
import numpy as np
import time
import urllib.request
import bu_alerts
from dateutil import relativedelta
from datetime import date, timedelta
from calendar import monthrange
from bu_snowflake import get_connection
# To get credentials used in process
from bu_config import get_config

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# log file location
log_file_location = os.getcwd() + '\\' + 'logs' + '\\' + 'PJM_GENOFFER_RAW.txt'
if os.path.isfile(log_file_location):
    os.remove(log_file_location)

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] - %(message)s',
    filename=log_file_location)

job_id=np.random.randint(1000000,9999999)

date_list=[]

def get_primary_keys(databasename,schemaname,tablename):
    try:
        primary_key_list = []
        table = databasename + '.' + schemaname + '.' + tablename
        query_primary_key = f'''SHOW PRIMARY KEYS IN {table}'''
        conn = get_connection(role='OWNER_{}'.format(databasename),database=databasename,schema=schemaname)
        # conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query_primary_key)
        result = cursor.fetchall()
        if len(result) > 0:
            for j in range(0, len(result)):
                primary_key_list.append(result[j][4])
        print("Primary keys for table are ", primary_key_list)
        return primary_key_list
    except Exception as e:
        print(f"Exception caught in get_primary_keys:::::: {e}")
        logging.exception(f'Exception caught in get_primary_keys:::::::: {e}')
        raise e

def get_dates(max_date,table_pjm_genoffer_raw):
    try:
        if max_date is None:
            start_date = datetime.date(2018, 1, 1)
        else:
            print(max_date)
            days=monthrange(max_date.year,max_date.month)[1]
            if max_date.day == days:
                start_date = (max_date+timedelta(days=1)).date()
            else:
                max_date01 = max_date.replace(day=1,hour=0)
                delete_query = f'''delete from {table_pjm_genoffer_raw} where DATE_BEGINNING_EPT>='{max_date01}' and DATE_BEGINNING_EPT<='{max_date}'
                '''
                result = query_execution_api(delete_query)
                print("After deleting existing records",result)
                start_date = max_date01.date()
                # max_date = max_date.replace(day=1,hour=0,minute=0)
                # start_date = (max_date+timedelta(days=days-1)).date()
                
        end_date = datetime.today().date()
        delta=relativedelta.relativedelta(months=1)
        end_date=(end_date-relativedelta.relativedelta(months=4))-timedelta(days=datetime.today().day)
        print(start_date)
        print(end_date)
        while (start_date<end_date):
            days_in_month=monthrange(start_date.year,start_date.month)[1]-1
            date_param=(str(start_date)+" 00:00 to "+str(start_date+timedelta(days=days_in_month))+" 23:59")
            date_list.append(date_param)
            start_date+=delta
        print(date_list)
        return date_list
    except Exception as e:
        print("Exception caught during execution: ",e)   
        logging.exception(f'Exception caught during execution: {e}')
        raise e

def query_execution_api(query):
    try:
        conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='ISO')
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Exception as e:
        print("Exception caught during execution: ",e)
        logging.exception(f'Exception caught during execution: {e}')
        cursor.close()
        raise e
        # return None


# Copy csv data using PUT-COPY command to snowflake
def copy_csv_to_database(pjm_csv_data):
    try:
        conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='ISO')
        # starting a transaction if any error will occur in this try block than rollback all the dml operations
        conn.cursor().execute('BEGIN')
        # removing earlier stage file from staging area
        print("Before remove staging file")
        conn.cursor().execute("USE DATABASE POWERDB")
        conn.cursor().execute("USE SCHEMA ISO")
        conn.cursor().execute('remove @%PJM_GENOFFER_RAW')
        print("After remove staging file")
        conn.cursor().execute("PUT file://{} @%PJM_GENOFFER_RAW overwrite=true".format(pjm_csv_data))
        print("After PUT command")
        conn.cursor().execute(f'''
                COPY INTO {table_pjm_genoffer_raw} file_format=(type=csv 
                skip_header=1 field_optionally_enclosed_by = '"' empty_field_as_null=true)
                ''')
        print("After COPY command")
    except Exception as e:
        print(f"error while executing put {e}")
        logging.exception(f'Exception caught during execution: {e}')
        conn.cursor().execute('rollback')           
        conn.close()
        raise e
    else:
        conn.cursor().execute('commit')
        conn.close()
        
# Fetch pjm data from api based on list of dates
def fetch_pjm_raw(list_of_dates):
    try:
        rowsInserted = 0
        for date in list_of_dates:
            dfs = []
            print("Date is .............",date)
            rowCount=50000
            startRow = 1
            headers = {
                'Host': 'api.pjm.com',
                'Content-Type': "application/json",
                'Ocp-Apim-Subscription-Key': credential_dict['API_KEY']
            }
            flag=True
            while flag:
                count = 0
                # import pdb;pdb.set_trace()
                file_location = os.getcwd()+'\\'+'pjm_data.csv'
                data_file = open(file_location, 'w') 
                csv_writer = csv.writer(data_file)
                # url = "https://api.pjm.com/api/v1/energy_market_offers?rowCount="+str(rowCount)+"&sort=bid_datetime_beginning_ept&Order=Asc&StartRow="+str(startRow)+"&isActiveMetadata=True&bid_datetime_beginning_ept="+date
                url = credential_dict['SOURCE_URL'].format(str(rowCount),str(startRow),date)
                response = requests.request("GET",url, headers=headers)
                if(response.status_code==200):
                    data=json.loads(response.text)
                    data_rows = data['totalRows']
                    startRow = startRow + rowCount
                    if startRow>data_rows:
                        flag = False
                    else:
                        flag = True
                    gen_offer_data = data['items']
                    print("Length of gen_offer_data is ::::::::",len(gen_offer_data))
                    if len(gen_offer_data) > 0:
                        for data in gen_offer_data:
                            # print("Length of Data is ::::::::",len(data))
                            del data['bid_datetime_beginning_utc']
                            if count == 0:
                                header = data.keys()
                                csv_writer.writerow(header)
                                count += 1
                                csv_writer.writerow(data.values())
                            else:
                                csv_writer.writerow(data.values())
                        # import pdb;pdb.set_trace()
                        data_file.close()
                        pjm_csv_data = pd.read_csv(file_location)
                        os.remove(file_location)
                        pjm_csv_data.columns = [x.upper() for x in pjm_csv_data.columns]
                        pjm_csv_data['BU_ISO'] = 'PJMISO'
                        pjm_csv_data['INSERT_DATE']=datetime.now().strftime("%Y-%m-%d")
                        pjm_csv_data['UPDATE_DATE']=datetime.now().strftime("%Y-%m-%d")
                        pjm_csv_data.rename(columns={'BID_DATETIME_BEGINNING_EPT':'DATE_BEGINNING_EPT','MIN_RUNTIME':'MIN_RUN_TIME'}, inplace=True)
                        print("PJM CSV columns ***********",pjm_csv_data,startRow)
                        pjm_csv_data = pjm_csv_data[['BU_ISO','DATE_BEGINNING_EPT', 'UNIT_CODE', 'BID_SLOPE_FLAG', 'MW1', 'MW2', 'MW3', 'MW4',
                        'MW5', 'MW6', 'MW7', 'MW8', 'MW9', 'MW10', 'BID1', 'BID2', 'BID3',
                        'BID4', 'BID5', 'BID6', 'BID7', 'BID8', 'BID9', 'BID10', 'NO_LOAD_COST',
                        'COLD_START_COST', 'INTER_START_COST', 'HOT_START_COST', 'MAX_DAILY_STARTS',
                        'MIN_RUN_TIME', 'MAX_ECOMAX', 'MIN_ECOMAX','AVG_ECOMAX',  'MAX_ECOMIN', 'MIN_ECOMIN',
                        'AVG_ECOMIN', 'INSERT_DATE','UPDATE_DATE']]
                        dfs.append(pjm_csv_data)
            print(len(dfs))
            if len(dfs) > 0:
                finaldf = pd.concat(dfs)
                primary_key_list = get_primary_keys(databasename,schemaname,tablename)
                print("Length of finaldf before drop_duplicates is :::::::::: ",len(finaldf))
                finaldf.drop_duplicates(subset=primary_key_list, inplace=True)
                print("Length of finaldf after drop_duplicates is :::::::::: ",len(finaldf))
                        # pjm_csv_location = os.getcwd()+'\\'+'pjm_csv_data.csv'
                pjm_csv_location = 'pjm_csv_data.csv'
                if os.path.isfile(pjm_csv_location):
                    os.remove(pjm_csv_location)
                finaldf.to_csv(pjm_csv_location, index=False,  quoting=csv.QUOTE_MINIMAL)
                copy_csv_to_database(pjm_csv_location)
                rowsInserted = rowsInserted + len(finaldf)            
                print("Copying CSV to DB completed,Inserted Rows:::",rowsInserted)
        return rowsInserted
    except Exception as e:
        print("Exception caught during execution",e)
        logging.exception(f'Exception caught during execution: {e}')
        raise e
       
if __name__ == "__main__":
    logging.info('Execution Started')
    starttime=datetime.now()
    rows=0
    try:
        credential_dict = get_config('PJMISO website data','PJM_GENOFFER_RAW')
        databasename = credential_dict['DATABASE']
        schemaname = credential_dict['TABLE_SCHEMA']
        tablename = credential_dict['TABLE_NAME']
        print(tablename)
        table_pjm_genoffer_raw = databasename +'.'+ schemaname +'.'+ tablename
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name=tablename,database=databasename,status='Started',table_name = table_pjm_genoffer_raw, row_count=rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=credential_dict['IT_OWNER'])
    
        if starttime.day>= 1 and starttime.day<=5:        
            query_max_date = f'''select max(DATE_BEGINNING_EPT) from {table_pjm_genoffer_raw}'''
            max_date = query_execution_api(query_max_date)[0][0]
            print("MAX DATE :::::", max_date)
            list_of_dates = get_dates(max_date,table_pjm_genoffer_raw)
            # list_of_dates = ['2020-11-01 00:00 to 2020-11-30 23:59']
            rows = fetch_pjm_raw(list_of_dates)
        else:
            print("Current date is greater than 5 so will not update the table ",table_pjm_genoffer_raw)
            logging.info('Current date is greater than 5 so will not update the table {}'.format(table_pjm_genoffer_raw))
        
        logging.info('Execution Done')
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name=tablename,database=databasename,status='Completed',table_name = table_pjm_genoffer_raw, row_count=rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=credential_dict['IT_OWNER']) 
        bu_alerts.send_mail(
            receiver_email = credential_dict['EMAIL_LIST'],
            # receiver_email = 'radha.waswani@biourja.com',
            mail_subject ='JOB SUCCESS - {}'.format(tablename),
            mail_body ='{} completed successfully, Attached logs'.format(tablename),
            attachment_location = log_file_location

        )   
    except Exception as e:
        print("Exception caught during execution: ",e)
        logging.exception(f'Exception caught during execution: {e}')
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name=tablename,database=databasename,status='Failed',table_name = table_pjm_genoffer_raw, row_count=0, log=log_json, warehouse='ITPYTHON_WH',process_owner=credential_dict['IT_OWNER']) 
        bu_alerts.send_mail(
            receiver_email = credential_dict['EMAIL_LIST'],
            # receiver_email = 'radha.waswani@biourja.com',
            mail_subject = 'JOB FAILED - {}'.format(tablename),
            mail_body = '{} failed during execution, Attached logs'.format(tablename),
            attachment_location = log_file_location
        )
    endtime=datetime.now()
    print('Complete work at {} ...'.format(endtime.strftime('%Y-%m-%d %H:%M:%S')))
    print('Total time taken: {} seconds'.format((endtime-starttime).total_seconds()))


