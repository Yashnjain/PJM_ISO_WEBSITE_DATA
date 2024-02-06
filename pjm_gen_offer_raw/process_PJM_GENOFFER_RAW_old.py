import snowflake.connector
from dateutil import relativedelta
from datetime import date, timedelta
from calendar import monthrange
# from connection import get_connection
from bulog import get_connection,bulog
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

sender_email = 'biourjapowerdata@biourja.com'
sender_password = r'bY3mLSQ-\Q!9QmXJ'
receiver_email = 'indiapowerit@biourja.com'
job_id=np.random.randint(1000000,9999999)

date_list=[]
# n = datetime.now()
# log file location
# print (os.getcwd());
log_path= os.getcwd() + '\\' + 'logs' + '\\' + 'PJM_GENOFFER_RAW.txt'
logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s [%(levelname)s] - %(message)s',
    filename=log_path)

table_pjm_genoffer_raw = 'POWERDB.ISO.PJM_GENOFFER_RAW'

def get_dates(max_date):
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
                print("After deleting existing records")
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

def query_execution_api(query):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Exception as e:
        print("Exception caught during execution: ",e)
        cursor.close()
        # return None


# Copy csv data using PUT-COPY command to snowflake
def copy_csv_to_database(pjm_csv_data):
    try:
        conn = get_connection()
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
        conn.cursor().execute('rollback')           
        conn.close()
    else:
        conn.cursor().execute('commit')
        conn.close()
        
# Fetch pjm data from api based on list of dates
def fetch_pjm_raw(list_of_dates):
    try:
        rowsInserted = 0
        for date in list_of_dates:
            print("Date is .............",date)
            rowCount=50000
            startRow = 1
            headers = {
                'Host': 'api.pjm.com',
                'Content-Type': "application/json",
                'Ocp-Apim-Subscription-Key': '1c3cfc89d2dd456fadd7493d6bf8e0a7'
            }
            flag=True
            while flag:
                count = 0
                # import pdb;pdb.set_trace()
                file_location = os.getcwd()+'\\'+'pjm_data.csv'
                data_file = open(file_location, 'w') 
                csv_writer = csv.writer(data_file)
                url = "https://api.pjm.com/api/v1/energy_market_offers?rowCount="+str(rowCount)+"&sort=bid_datetime_beginning_ept&Order=Asc&StartRow="+str(startRow)+"&isActiveMetadata=True&bid_datetime_beginning_ept="+date
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
                    # pjm_csv_location = os.getcwd()+'\\'+'pjm_csv_data.csv'
                    pjm_csv_location = 'pjm_csv_data.csv'
                    if os.path.isfile(pjm_csv_location):
                        os.remove(pjm_csv_location)
                    pjm_csv_data.to_csv(pjm_csv_location, index=False,  quoting=csv.QUOTE_MINIMAL)
                    copy_csv_to_database(pjm_csv_location)
                    rowsInserted = rowsInserted + len(pjm_csv_data)            
            print("Copying CSV to DB completed,Inserted Rows:::",rowsInserted)
        return rowsInserted
    except Exception as e:
        print("Exception caught",e)
       
if __name__ == "__main__":
    starttime=datetime.now()
    rows=0
    log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
    bulog(process_name="PJM_GENOFFER_RAW",status='Started',table_name='POWERDB.ISO.PJM_GENOFFER_RAW', row_count=rows, log=log_json, warehouse='ITPYTHON_WH')
    
    logging.warning('Start work at {} ...'.format(starttime.strftime('%Y-%m-%d %H:%M:%S')))
    try:
        if starttime.day>= 1 and starttime.day<=5:        
            query_max_date = f'''select max(DATE_BEGINNING_EPT) from {table_pjm_genoffer_raw}'''
            max_date = query_execution_api(query_max_date)[0][0]
            list_of_dates = get_dates(max_date)
            # list_of_dates = ['2020-07-01 00:00 to 2020-07-31 23:59']
            rows = fetch_pjm_raw(list_of_dates)
        else:
            print("Current date is greater than 5 so will not update the table ",table_pjm_genoffer_raw)
            logging.warning('Current date is greater than 5 so will not update the table {}'.format(table_pjm_genoffer_raw))

        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bulog(process_name="PJM_GENOFFER_RAW",status='Completed',table_name='POWERDB.ISO.PJM_GENOFFER_RAW', row_count=rows, log=log_json, warehouse='ITPYTHON_WH') 
        bu_alerts.send_mail(sender_email,sender_password,receiver_email,mail_subject='Job ran successfully - PJM_GENOFFER_RAW',
            mail_body='Successfully executed PJM_GENOFFER_RAW job'
        )   
    except Exception as e:
        print("Exception caught during execution: ",e)
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bulog(process_name="PJM_GENOFFER_RAW",status='FAILED',table_name='POWERDB.ISO.PJM_GENOFFER_RAW', row_count=0, log=log_json, warehouse='ITPYTHON_WH') 
        bu_alerts.send_mail(sender_email,sender_password,receiver_email,mail_subject='Job failed - PJM_GENOFFER_RAW',
            mail_body=f'Failed during execution {e}'
        )
    endtime=datetime.now()
    logging.warning('Complete work at {} ...'.format(endtime.strftime('%Y-%m-%d %H:%M:%S')))
    logging.warning('Total time taken: {} seconds'.format((endtime-starttime).total_seconds()))


