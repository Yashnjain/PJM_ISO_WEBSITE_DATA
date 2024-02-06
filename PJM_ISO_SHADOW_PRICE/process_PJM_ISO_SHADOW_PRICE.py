import logging
import os 
import requests
import json
import csv 
import pandas as pd
import time
import urllib.request
import numpy as np
import bu_alerts
from datetime import datetime
from dateutil import relativedelta
from datetime import date, timedelta
from calendar import monthrange
from bu_snowflake import get_connection
# To get credentials used in process
from bu_config import get_config

# Getting list of dates for which data to be fetched
def get_dates(max_date,market,table_pjm_iso_shadow_price):
    try:
        date_list=[]
        if max_date is None:
            # Set date for historical data(from January 2017)
            start_date = datetime.date(2017, 1, 1)
        else:
            print("Max date from database records is ::::",max_date)

            days = monthrange(max_date.year,max_date.month)[1]

            if max_date.day == days:
                start_date = (max_date+timedelta(days=1)).date()
            else:
                # Condition: Due to any reason when process was stopped and could not insert data for complete month
                # Get first day of month for the broken data
                max_date01 = max_date.replace(day=1,hour=0)
                # Query to delete data from table for the broken data
                delete_query = f'''delete from {table_pjm_iso_shadow_price} where DATETIME_HR_BEG>='{max_date01}' and DATETIME_HR_BEG<='{max_date}' and MARKET_TYPE = '{market}' '''

                result = query_execution_api(delete_query)
                print("After deleting existing records",result)
                # Set start_date for again execute process for the same month
                start_date = max_date01.date()
        # Set end_date to the today's date
        end_date = datetime.today().date()
        # Create delta object to get duration of 1 month
        delta = relativedelta.relativedelta(months=1)
        # end_date=(end_date-relativedelta.relativedelta(months=1))
        print("Start Date ::::",start_date)
        print("End Date::::::",end_date)
        while (start_date<end_date):
            days_in_month = monthrange(start_date.year,start_date.month)[1]-1
            date_param = (str(start_date)+" 00:00 to "+str(start_date+timedelta(days=days_in_month))+" 23:59")
            date_list.append(date_param)
            start_date += delta
        print("Dates List is ::::::::::",date_list)
        return date_list
    except Exception as e:
        print("Exception caught in get_dates(): ",e)
        logging.exception(f'Exception caught in get_dates(): {e}')
        raise e
    

def query_execution_api(query):
    try:
        conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='ISO')
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Exception as e:
        print("Exception caught in query_execution_api(): ",e)
        logging.exception(f'Exception caught in query_execution_api(): {e}')
        cursor.close()
        raise e

# Copy csv data using PUT-COPY command to snowflake
def copy_csv_to_database(pjm_csv_data):
    try:
        conn = get_connection(role=f'OWNER_{databasename}',database = databasename,schema=schemaname)
        # starting a transaction if any error will occur in this try block than rollback all the dml operations
        conn.cursor().execute('BEGIN')
        # removing earlier stage file from staging area
        print("Before remove staging file")
        conn.cursor().execute(f"USE DATABASE {databasename}")
        conn.cursor().execute(f"USE SCHEMA {schemaname}")
        conn.cursor().execute(f'remove @%{tablename}')
        print("After remove staging file")
        conn.cursor().execute(f"PUT file://{pjm_csv_data} @%{tablename} overwrite=true")
        conn.cursor().execute(f'''
                COPY INTO {tablename} file_format=(type=csv
                skip_header=1 field_optionally_enclosed_by = '"' empty_field_as_null=true)
                ''')
        print("After COPY command")
    except Exception as e:
        conn.cursor().execute('rollback')
        conn.close()
        logging.exception(f'Exception caught in copy_csv_to_database(): {e}')
        raise e
    else:
        conn.cursor().execute('commit')
        
# Fetch pjm data from api based on list of dates amd market_type
def fetch_pjm_shadow(list_of_dates,market):
    try:
        rowsInserted = 0
        for date in list_of_dates:
            rowCount=50000
            startRow = 1
            headers = {
                'Host': 'api.pjm.com',
                'Content-Type': "application/json",
                'Ocp-Apim-Subscription-Key': credential_dict['API_KEY']
            }
            count = 0
            file_location = os.getcwd()+'\\'+'pjm_data.csv'
            data_file = open(file_location, 'w') 
            csv_writer = csv.writer(data_file)
            print("Market Type is ...........",market)
            # url = "https://api.pjm.com/api/v1/"+market.lower()+"_marginal_value?rowCount="+str(rowCount)+"&sort=datetime_beginning_ept&Order=Asc&StartRow="+str(startRow)+"&isActiveMetadata=True&datetime_beginning_ept="+date
            url = credential_dict['SOURCE_URL'].format(market.lower(),str(rowCount),str(startRow),date)
            print("Url :::::::",url)
            response = requests.request("GET",url, headers=headers)
            if(response.status_code==200):
                data=json.loads(response.text)
                shadow_price_data = data['items']
                print("Shadow price data ...",len(shadow_price_data),data['totalRows'])
                if len(shadow_price_data)>0 and data['totalRows']>0:
                    for data in shadow_price_data:
                        print(data)
                        del data['datetime_beginning_utc']
                        del data['datetime_ending_utc']
                        del data['datetime_ending_ept']
                        data['market_type']= market
                        if count == 0:
                            header = data.keys()
                            csv_writer.writerow(header)
                            count += 1
                            csv_writer.writerow(data.values())
                        else:
                            csv_writer.writerow(data.values())
                else:
                    break
            data_file.close()
            # import pdb;pdb.set_trace()
            pjm_csv_data = pd.read_csv(file_location)
            os.remove(file_location)
            # Perform calculations: 
            # RT is available for some 5 mins intervals and needs to be average of shadow_price to get the hourly value
            if(market=='RT'):
                df1 = pjm_csv_data.copy()
                df2 = pjm_csv_data.copy()
                df2['date'] = pd.to_datetime(df1['datetime_beginning_ept']).dt.strftime('%Y-%m-%d')
                df2['hour'] = pd.to_datetime(df1['datetime_beginning_ept']).dt.strftime('%H')
                df2 = df2.groupby(['date','hour','market_type','monitored_facility','contingency_facility'],sort=False,as_index=False).agg({'shadow_price':'mean'})
                df2['datetime_beginning_ept'] = pd.to_datetime(df2['date'] + ' ' + df2['hour'])
                del df2['date']
                del df2['hour']
                pjm_csv_data = df2
            elif market=='DA':
                df1 = pjm_csv_data.copy()
                df2 = df1.groupby(['datetime_beginning_ept','monitored_facility','contingency_facility','market_type'],sort=False, as_index=False).agg({'shadow_price':'sum'})
                pjm_csv_data = df2
            pjm_csv_data.columns = [x.upper() for x in pjm_csv_data.columns]
            pjm_csv_data['BU_ISO'] = 'PJMISO'
            pjm_csv_data['INSERT_DATE']=datetime.now().strftime("%Y-%m-%d")
            pjm_csv_data['UPDATE_DATE']=datetime.now().strftime("%Y-%m-%d")
            # Rename columns
            pjm_csv_data.rename(columns={'DATETIME_BEGINNING_EPT':'DATETIME_HR_BEG','SHADOW_PRICE':'SHADOW_PRICES','CONTINGENCY_FACILITY':'CONTINGENT_FACILITY'}, inplace=True)
            # Reorder columns
            pjm_csv_data = pjm_csv_data[['BU_ISO','DATETIME_HR_BEG', 'MARKET_TYPE', 'MONITORED_FACILITY', 'CONTINGENT_FACILITY','SHADOW_PRICES', 'INSERT_DATE','UPDATE_DATE']]
            print("PJM CSV columns ***********",pjm_csv_data)
            pjm_csv_location = 'pjm_csv_data.csv'
            if os.path.isfile(pjm_csv_location):
                os.remove(pjm_csv_location)
            pjm_csv_data.to_csv(pjm_csv_location, index=False,  quoting=csv.QUOTE_MINIMAL)
            copy_csv_to_database(pjm_csv_location)
            rowsInserted = rowsInserted + len(pjm_csv_data)
            # if os.path.isfile(pjm_csv_location):
            #     os.remove(pjm_csv_location)
        print("Copying CSV to DB completed,Inserted Rows:::",rowsInserted)
        return rowsInserted
    except Exception as e:
        print("Exception caught in fetch_pjm_shadow(): ",e)
        logging.exception(f'Exception caught in fetch_pjm_shadow(): {e}')
        raise e


if __name__ == "__main__":
    starttime=datetime.now()
    rows=0
    try:
        job_id=np.random.randint(1000000,9999999)
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        # log file location
        log_file_location = os.getcwd() + '\\logs\\' + 'PJM_ISO_SHADOW_PRICE.txt'
        if os.path.isfile(log_file_location):
            os.remove(log_file_location)

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] - %(message)s',
            filename=log_file_location)
        logging.info('Execution Started')
        credential_dict = get_config('PJMISO website data','PJM_ISO_SHADOW_PRICE')
        databasename = credential_dict['DATABASE']
        schemaname = credential_dict['TABLE_SCHEMA']
        tablename = credential_dict['TABLE_NAME']
        print(tablename)
        table_pjm_iso_shadow_price = databasename +'.'+ schemaname +'.'+ tablename
        print(table_pjm_iso_shadow_price)
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name=tablename,database=databasename,status='Started',table_name=table_pjm_iso_shadow_price, row_count=rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=credential_dict['IT_OWNER'])
        market_types = ['DA','RT']
        # market_types = ['RT']
        for market in market_types:
            query_max_date = f'''select max(DATETIME_HR_BEG) from {table_pjm_iso_shadow_price} where MARKET_TYPE = '{market}' '''
            max_date = query_execution_api(query_max_date)[0][0]
            list_of_dates = get_dates(max_date,market,table_pjm_iso_shadow_price)
            rows = rows + fetch_pjm_shadow(list_of_dates,market)
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name=tablename,database=databasename,status='Completed',table_name=table_pjm_iso_shadow_price, row_count=rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=credential_dict['IT_OWNER']) 
        logging.info('Execution Done')
        bu_alerts.send_mail(
            receiver_email = credential_dict['EMAIL_LIST'],
            # receiver_email = 'radha.waswani@biourja.com',
            mail_subject ='JOB SUCCESS - {}'.format(tablename),
            mail_body = '{} completed successfully, Attached logs'.format(tablename),
            attachment_location = log_file_location
        )
    except Exception as e:
        print("Exception caught during execution: ",e)
        logging.exception(f'Exception caught during execution: {e}')
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name=tablename,database=databasename,status='Failed',table_name= table_pjm_iso_shadow_price, row_count=0, log=log_json, warehouse='ITPYTHON_WH',process_owner=credential_dict['IT_OWNER']) 
        bu_alerts.send_mail(
            receiver_email = credential_dict['EMAIL_LIST'],
            # receiver_email = 'radha.waswani@biourja.com',
            mail_subject='JOB FAILED - {}'.format(tablename),
            mail_body='{} failed during execution, Attached logs'.format(tablename),
            attachment_location = log_file_location
        )
    endtime=datetime.now()
    print('Complete work at {} ...'.format(endtime.strftime('%Y-%m-%d %H:%M:%S')))
    print('Total time taken: {} seconds'.format((endtime-starttime).total_seconds()))


