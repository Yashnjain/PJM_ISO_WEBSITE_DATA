import requests
import snowflake.connector
import csv
import os
import pandas as pd
from datetime import datetime
import numpy as np
from bulog import get_connection,bulog,bumail
import logging
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
warehouse='ITPYTHON_WH'
# log file location
log_file_location=os.getcwd() + '\\' + 'logs' + '\\' + 'PJM_500KV_LOG.txt'
logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s [%(levelname)s] - %(message)s',
    filename=log_file_location)

#tablename='POWERDB_DEV.ISO.PJM_500KV_MAPPING'
tablename='POWERDB.ISO.PJM_500KV_MAPPING'
job_id=np.random.randint(1000000,9999999)
json='[{"JOB_ID": '+str(job_id)+', "DATE":"'+datetime.now().strftime('%Y-%m-%d')+'"}]'

def get_max_release_date(auction_type:str):
    """  Here we get max release date for particular auction type

    Args:
        auction_type (str): will be MONTHLY or ANNUAL

    Returns:
        dataframe : will return a dataframe
    """

    sql=f'''select max(RELEASE_DATE) AS RELEASE_DATE,AUCTION_TYPE from {tablename}
                               where AUCTION_TYPE='{auction_type}'
                               group by AUCTION_TYPE  '''
    try:
        conn=get_connection()
        cur=conn.cursor()
        cur.execute(sql)
        df_max_date=pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
    except Exception as e:
        print(f"error {e} in fetching max date for {auction_type}")
        cur.close()
        return None
    else:
        cur.close()
        return df_max_date

def get_pjm_auctionwise_file(auction_type:str):
    """ This function will download file for PJM_500KV_MAPPING TABLE
    based on auction type "MONTHLY" AND "ANNUAL"

    Args:
        auction_type (str): [description]

    Returns:
        [string or dataframe]: this will return dataframe if new record fetched from website
        
    """
    try:
        
        bulog(process_name=f'PJM_500KV_MAPPING-{auction_type}',status='Started',table_name=f'{tablename}', row_count=0, log=json, warehouse=warehouse)
      
        # here downloaded file will store in this folder
        # extract_dir_name='download'
        # downloaded file name based on auction type
        file_name=f"PJM_500KV_MAPPING_{auction_type}.csv"
        # file location path 
        file_location = os.getcwd()+'\\'+file_name
        

        # based on auction_type will select different url
        if auction_type=='ANNUAL':
            url="http://www.pjm.com/pub/account/auction-user-info/model-annual/Annual-500kv-mapping.csv"
        else:
            url="http://www.pjm.com/pub/account/auction-user-info/model-monthly/500kv-mapping.csv"
            

        file_data=requests.get(url)
        # if file exist than first delete
        if os.path.isfile(file_location):
            os.remove(file_location)
        # writing the file content to local folder file 
        with open(file_location,'wb') as file_name:
            file_name.write(file_data.content)
        # reading the posted_date from the downloaded file 
        posted_date=pd.read_csv(file_location,nrows=0)
        posted_date = posted_date.columns[0][9:]
        posted_date=datetime.strptime(posted_date,'%Y%m%d').strftime('%Y-%m-%d')

      
        
    except Exception as e:
        bumail(['indiapowerit@biourja.com'], 'PJM_500KV_FAILED' , 'PJM_500KV_FAILED,ERROR IS - {}'.format(e) )
     
        
        print(f"got error {e} while fetching data for {auction_type} type")
        return f"got error {e} while fetching data for {auction_type} type"
    else:
        # get max release  date from the "powerdb" database 
        max_release_date=get_max_release_date(auction_type)
        max_release_date=max_release_date['RELEASE_DATE'][0]
        max_release_date_db= max_release_date.strftime('%Y-%m-%d')
        
        if max_release_date_db==posted_date:
            # process abandon if database date and posted date are equal
            bulog(process_name=f'PJM_500KV_MAPPING-{auction_type}',status='Completed',table_name=f'{tablename}', row_count=0, log=json, warehouse=warehouse)
         
            return f"no_new_records for {auction_type} auction"

        auction_new_record=pd.read_csv(file_location,header=1)
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
        try:
            # csv name it will upload to sf
            snow_upload_csv=f"SNOW_PJM_500KV_MAPPING_{auction_type}.csv"
            # snow_upload_csv_location = os.getcwd() + '\\' + extract_dir_name + '\\'+snow_upload_csv
            # auction_new_record.to_csv(snow_upload_csv, index=False, date_format='%Y-%m-%d', quoting=csv.QUOTE_MINIMAL)
            auction_new_record.to_csv(snow_upload_csv, index=False,  quoting=csv.QUOTE_MINIMAL)
            conn=get_connection()
            # starting a transaction if any error will occur in this try block than rollback all the dml operations
            conn.cursor().execute('BEGIN')
            
            
            
            # removing earlier stage file from staging area
            conn.cursor().execute('remove @%PJM_500KV_MAPPING')
            
            conn.cursor().execute("PUT file://{} @%PJM_500KV_MAPPING overwrite=true".format(snow_upload_csv))
            conn.cursor().execute('''
                    COPY INTO PJM_500KV_MAPPING file_format=(type=csv 
                    skip_header=1 field_optionally_enclosed_by = '"' empty_field_as_null=true)
                    ''')
            # this query will update model_idx column in such a way earlier records of model_idx subtracted by -1
            conn.cursor().execute(f''' UPDATE POWERDB.iso.PJM_500KV_MAPPING pd 
            SET MODEL_IDX = ((ps.row_number * -1) +1)
                    from (
                    select dense_rank()
                    over (order by RELEASE_DATE desc) as row_number, RELEASE_DATE from  POWERDB.iso.PJM_500KV_MAPPING where AUCTION_TYPE = '{auction_type}'
                    group by RELEASE_DATE
                    )ps
                    WHERE pd.RELEASE_DATE= ps.RELEASE_DATE and 
                    pd.AUCTION_TYPE = '{auction_type}' ''')
        except Exception as e:
            bumail(['indiapowerit@biourja.com'], 'PJM_500KV_FAILED' , 'PJM_500KV_FAILED,ERROR IS - {}'.format(e) )
     
            print(f"error while executing put {e}")
            conn.cursor().execute('rollback')           
            conn.close()
        else:
            conn.cursor().execute('commit')
            conn.close()
            # this dataframe retuns when new records found 
            bulog(process_name=f'PJM_500KV_MAPPING-{auction_type}',status='Completed',table_name=f'{tablename}', row_count=len(auction_new_record), log=json, warehouse=warehouse)
         
            return auction_new_record

        



    








if __name__ == "__main__": 
# getting both auction file "MONTHLY" AND "ANNUAL"
    starttime=datetime.now()
    logging.warning('Start work at {} ...'.format(starttime.strftime('%Y-%m-%d %H:%M:%S')))
    auction_type_list=['ANNUAL','MONTHLY']
    try:

        for auction_type in auction_type_list:
            result=get_pjm_auctionwise_file(auction_type)
            # here we check above result variable if it is dataframe it means new records fetched than print number of records
            if isinstance(result, pd.DataFrame):
                print(f"new rows inserted for {auction_type}  {len(result)}")
                
            else:
                
                print(result)

    except Exception as e:
        bumail(['indiapowerit@biourja.com'], 'PJM_500KV_FAILED' , 'PJM_500KV_FAILED,ERROR IS - {}'.format(e) )
        bulog(process_name='PJM_500KV_MAPPING',status='FAILED',table_name=f'{tablename}', row_count=0, log=json, warehouse=warehouse)
         
        print(e)

    endtime=datetime.now()
    logging.warning('Complete work at {} ...'.format(endtime.strftime('%Y-%m-%d %H:%M:%S')))
    logging.warning('Total time taken: {} seconds'.format((endtime-starttime).total_seconds()))