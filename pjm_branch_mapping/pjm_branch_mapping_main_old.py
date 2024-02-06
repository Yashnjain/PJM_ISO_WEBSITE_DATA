# -*- coding: utf-8 -*-
"""
Created on Fri Jul 24 18:02:15 2020

@author: Manish.Gupta
@description: this script will be fetch the data for PJM_BRANCH_MAPPING table from pjm api and push into snowflake database
"""

import requests
from bulog import get_connection
import pandas as pd
import csv
import os
from datetime import datetime, date
import logging

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
    
# log file location
log_file_location=os.getcwd() + '\\' + 'logs' + '\\' + 'PJM_BRANCH_MAPPING'
logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s [%(levelname)s] - %(message)s',
    filename=log_file_location)

#define gloabal variable 
#tablename = "POWERDB_DEV.PTEST.PJM_BRANCH_MAPPING"
tablename = "POWERDB.ISO.PJM_BRANCH_MAPPING"

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
        print(df_max_date)
    except Exception as e:
        print(f"error {e} in fetching max date for {auction_type}")
        cur.close()
        return None
    else:
        cur.close()
        return df_max_date

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
        # here downloaded file will store in this folder
        extract_dir_name='download'
        # downloaded file name based on auction type
        file_name=f"PJM_BRANCH_MAPPING_{auction_type}.csv"
        #print("File Name : {}".format(file_name))
        # file location path 
        #file_location = os.getcwd() + '\\' + extract_dir_name + '\\'+file_name
        file_location = os.getcwd() + '\\' + file_name
        #print("File Location : {}".format(file_location))
        
        # based on auction_type will select different url
        if auction_type=='ANNUAL':
            url="http://www.pjm.com/pub/account/auction-user-info/model-annual/Annual-PSSE-Branch-Mapping-File.csv"
        else:
            url="http://www.pjm.com/pub/account/auction-user-info/model-monthly/PSSE-Branch-Mapping-File.csv"
        #print("URL : {}".format(url))
        
        file_data=requests.get(url)
        print(file_data)
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
        print("Posted Date : {}".format(posted_date))  
    except Exception as e:
        print(f"got error {e} while fetching data for {auction_type} type")
        return f"got error {e} while fetching data for {auction_type} type"
    else:
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
        
        if max_release_date_db==posted_date:
            # process abandon if database date and posted date are equal
            return f"no_new_records for {auction_type} auction"
        
        #read the downloaded csv file 
        print("read the downloaded csv file")
        new_auction_data = pd.read_csv(file_location, header=2)
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
        try:
            sf_upload_csv=f"SF_PJM_BRANCH_MAPPING_{auction_type}.csv"
            #sf_upload_csv_location = os.getcwd() + '\\' + extract_dir_name + '\\'+sf_upload_csv
            sf_upload_csv_location = sf_upload_csv
            print(sf_upload_csv_location)
            updated_auction_data.to_csv(sf_upload_csv_location, index=False, date_format='%Y-%m-%d', quoting=csv.QUOTE_MINIMAL)
            #get snowflake connection
            conn=get_connection()
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
            print(f"error while executing put {e}")
            conn.cursor().execute('rollback')           
            conn.close()
        else:
            conn.cursor().execute('commit')
            conn.close()
        return updated_auction_data
    return f'NO NEW RECORD FOR THIS {auction_type} TYPE'
       
    
if __name__ == "__main__":
    starttime=datetime.now()
    logging.warning('Start proccess for PJM_BRANCH_MAPPING at {} ...'.format(starttime.strftime('%Y-%m-%d %H:%M:%S')))
    auction_type_list=['ANNUAL','MONTHLY']
    try:
         for auction_type in auction_type_list:
            result=get_df_pjm_branch_mapping(auction_type)
            # here we check above result variable if it is dataframe it means new records fetched than print number of records
            if isinstance(result, pd.DataFrame):
                print(f"new rows inserted for {auction_type}  {len(result)}")
            else:
                print(result)
    except Exception as e:
        print(e)
               
    endtime=datetime.now()
    logging.warning('Complete work at {} ...'.format(endtime.strftime('%Y-%m-%d %H:%M:%S')))
    logging.warning('Total time taken: {} seconds'.format((endtime-starttime).total_seconds()))            
    print("End proccess for PJM_BRANCH_MAPPING")
    
