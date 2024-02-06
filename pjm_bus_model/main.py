import requests
import snowflake.connector
import csv
import os
import pandas as pd
from datetime import datetime
import numpy as np
from bulog import get_connection ,bulog,bumail
import logging
from selenium.webdriver import Chrome,ChromeOptions
import time 
import os 
import itertools 
warehouse='ITPYTHON_WH'
job_id=np.random.randint(1000000,9999999)
json='[{"JOB_ID": '+str(job_id)+', "DATE":"'+datetime.now().strftime('%Y-%m-%d')+'"}]'
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# log file location
log_file_location=r'S:\IT Dev\Production_Environment\pjm_bus_model\logs\pjm_bus_model_log.txt'
logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s [%(levelname)s] - %(message)s',
    filename=log_file_location)

# login credentials
username='7139299183'
password='Biourja_1482'
annual='ANNUAL'
monthly='MONTHLY'
# xpath for date and annual files 
monthly_date_path='//*[@id="content"]/article/table[1]/tbody/tr[1]/td[2]/div'
annual_date_path='//*[@id="content"]/article/table[2]/tbody/tr[1]/td[2]/div'
monthly_file_path='//*[@id="content"]/article/table[1]/tbody/tr[1]/td[1]/a'
annual_file_path='//*[@id="content"]/article/table[2]/tbody/tr[1]/td[1]/a'
tablename='POWERDB.ISO.PJM_BUS_MODEL'

# login url
weburl=r'https://pjm.com/login.aspx?returnUrl=%2Fmarkets-and-operations%2Fftr%2Fprivate-ftr-model-ceii-data.aspx'
# allfile location path 
#files_location = os.getcwd()+'\\'+'download'
files_location = os.getcwd()
# chrome browser path 
#weddriverpath=r'S:\IT Dev\Production_Environment\pjm_bus_model\chrome_driver\84.0.4147.30\chromedriver.exe'
weddriverpath=r'C:\Python3.8.5\chromedriver.exe'
options = ChromeOptions() 
options.add_experimental_option("prefs", {
        "download.default_directory": files_location,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing_for_trusted_sources_enabled": False,
        "detach": True,
        "safebrowsing.enabled": False
})


browser = Chrome(chrome_options=options ,executable_path=weddriverpath)

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
        if len(df_max_date)>0:
            max_release_date=df_max_date['RELEASE_DATE'][0]
            return max_release_date
            
        else:
            return ''
def login():
    try:
        
        browser.get(weburl)
        print(browser.title)
        browser.find_element_by_name("body_0$ctl00$txtUserName").send_keys(username)
        browser.find_element_by_name("body_0$ctl00$txtPassword").send_keys(password)
        browser.find_element_by_name("body_0$ctl00$btnLogin").click()
        return  None
    except Exception as e:
        bumail(['indiapowerit@biourja.com'], 'PJM_BUS_MODEL_FAILED' , 'PJM_BUS_MODEL_FAILED WHILE LOGIN,ERROR IS - {}'.format(e) )
       
        print(f"error found while login to website {e}")

def bus_model(auction_type:str,date_path:str,file_path:str):
    """This function will get date from website than check with database release date
       if new date arrive file will get downloaded and transform the downloaded file 
       as per buisness logic and upload it to snowflake for both  annual and monthly 
       auction type 

    Args:
        auction_type (str): monthly or annual
        date_path (str): website date location xpath
        file_path (str): website file location xpath

    Returns:
        either string or dataframe: if new record inserted than dataframe will return otherwise string retrun
    """
    try:
        
        print(auction_type)
        bulog(process_name=f'PJM_BUS_MODEL-{auction_type}',status='Started',table_name=f'{tablename}', row_count=0, log=json, warehouse=warehouse)
      

        auctionfile=''
        # getiing release date from web 
        max_release_date_db=get_max_release_date(auction_type)
        

        if max_release_date_db=='':
            max_release_date_db
        else:
            max_release_date_db=max_release_date_db.strftime('%Y-%m-%d')
        webfiledate=browser.find_element_by_xpath(date_path).text
        webfiledate=webfiledate.replace('.','-')
        webfiledate=datetime.strptime(webfiledate,'%m-%d-%Y').strftime('%Y-%m-%d')
        if webfiledate > max_release_date_db:
            files=os.listdir(files_location)
            # removing existing files 
            for file in files :
                os.remove(files_location+'\\'+file)
            browser.find_element_by_xpath(file_path).click()
            time.sleep(90)
            #getting downloaded file     
            files=os.listdir(files_location)
            for file in files:
                if '.raw' in file:

                    auctionfile=file.replace('.raw','.txt')
                    os.rename(files_location+'//'+file,files_location+'//'+auctionfile)
            # after downloading reading file and extracting all the required section of file 
            with open(files_location+'//'+auctionfile) as fp:
                

                Lines = fp.readlines() 
            #     print(str(Lines).strip())

                index_value={}
                count=0
                for line in Lines: 
            
                    if "FULL COPY OF NETMOM FROM" in line.upper():
                        index_value[count] = 'BEGIN_BUS_DATA'
            #             print(index_value)
                    elif "END OF BUS DATA" in line.upper() and "BEGIN LOAD DATA" in line.upper() :
                        index_value[count] = 'END_OF_BUS_DATA_BEGIN_LOAD'
            #         elif "BEGIN LOAD DATA" in line.upper():
            #             index_value[count] = 'BEGIN LOAD DATA'
                    elif "END OF LOAD DATA" in line.upper() and "BEGIN GENERATOR DATA" in line.upper():
                        index_value[count] = 'END_OF_LOAD_DATA_BEGIN_GEN'
                        
            #         elif "BEGIN GENERATOR DATA" in line.upper():
            #             index_value[count] = 'BEGIN GENERATOR DATA'
                    elif "END OF GENERATOR DATA" in line.upper():
                        index_value[count] = 'END OF GENERATOR DATA'
                
                    elif "BEGIN AREA INTERCHANGE DATA" in line.upper():
                        index_value[count] = 'BEGIN AREA INTERCHANGE DATA'
                    elif "END OF AREA INTERCHANGE DATA" in line.upper():
                        index_value[count] = 'END OF AREA INTERCHANGE DATA'
                    elif "BEGIN ZONE DATA" in line.upper():
                        index_value[count] = 'BEGIN ZONE DATA'
                    elif "END OF ZONE DATA" in line.upper():
                        index_value[count] = 'END OF ZONE DATA'
                    elif "BEGIN OWNER DATA" in line.upper():
                        index_value[count] = 'BEGIN OWNER DATA'
                    elif "END OF OWNER DATA" in line.upper():
                        index_value[count] = 'END OF OWNER DATA'
                    
                    
                    count += 1
                print(index_value)
            
            
            if os.path.exists(files_location+'//'+"bus_data_extaract.txt"):
                os.remove(files_location+'//'+"bus_data_extaract.txt")
            # extracting bus data from webfile
            if 'BEGIN_BUS_DATA' and 'END_OF_BUS_DATA_BEGIN_LOAD' in index_value.values():
                bus_begin_key=list(index_value.keys())[list(index_value.values()).index('BEGIN_BUS_DATA')]
                bus_end_key=list(index_value.keys())[list(index_value.values()).index('END_OF_BUS_DATA_BEGIN_LOAD')]
            #     print(bus_begin_key,bus_end_key)
                with open(files_location+'//'+auctionfile, "r") as text_file:
                    

                    for line in itertools.islice(text_file, bus_begin_key+1,bus_end_key):
                        with open(files_location+'//'+"bus_data_extaract.txt", "a") as outputbus:
                            outputbus.write(line)
                            
            #extracting load data  from file                
            if os.path.exists(files_location+'//'+"load_data_extract.txt"):
                os.remove(files_location+'//'+"load_data_extract.txt")

            if 'END_OF_BUS_DATA_BEGIN_LOAD' and 'END_OF_LOAD_DATA_BEGIN_GEN' in index_value.values():
                load_begin_key=list(index_value.keys())[list(index_value.values()).index('END_OF_BUS_DATA_BEGIN_LOAD')]
                load_end_key=list(index_value.keys())[list(index_value.values()).index('END_OF_LOAD_DATA_BEGIN_GEN')]
            #     print(bus_begin_key,bus_end_key)
                with open(files_location+'//'+auctionfile, "r") as text_file:
                    

                    for line in itertools.islice(text_file, load_begin_key+1,load_end_key):
                        with open(files_location+'//'+"load_data_extract.txt", "a") as load_data:
                            load_data.write(line)
                            
            #extracting gen data  from file                  
            if os.path.exists(files_location+'//'+"gen_data_extaract.txt"):
                os.remove(files_location+'//'+"gen_data_extaract.txt")

            if 'END_OF_LOAD_DATA_BEGIN_GEN' and 'END OF GENERATOR DATA' in index_value.values():
                gen_begin_key=list(index_value.keys())[list(index_value.values()).index('END_OF_LOAD_DATA_BEGIN_GEN')]
                gen_end_key=list(index_value.keys())[list(index_value.values()).index('END OF GENERATOR DATA')]
            #     print(bus_begin_key,bus_end_key)
                with open(files_location+'//'+auctionfile, "r") as text_file:
                    

                    for line in itertools.islice(text_file, gen_begin_key+1,gen_end_key):
                        with open(files_location+'//'+"gen_data_extaract.txt", "a") as gen_data_extaract:
                            gen_data_extaract.write(line)

            #extracting BEGIN AREA INTERCHANGE DATA  from file 

            if os.path.exists(files_location+'//'+"area_data_extaract.txt"):
                os.remove(files_location+'//'+"area_data_extaract.txt")

            if 'BEGIN AREA INTERCHANGE DATA' and 'END OF AREA INTERCHANGE DATA' in index_value.values():
                area_begin_key=list(index_value.keys())[list(index_value.values()).index('BEGIN AREA INTERCHANGE DATA')]
                area_end_key=list(index_value.keys())[list(index_value.values()).index('END OF AREA INTERCHANGE DATA')]
            #     print(bus_begin_key,bus_end_key)
                with open(files_location+'//'+auctionfile, "r") as text_file:
                    

                    for line in itertools.islice(text_file, area_begin_key+1,area_end_key):
                        with open(files_location+'//'+"area_data_extaract.txt", "a") as area_data_extaract:
                            area_data_extaract.write(line)
                            
            #extracting BEGIN zone DATA  from file 
            if os.path.exists(files_location+'//'+"zone_data_extaract.txt"):
                os.remove(files_location+'//'+"zone_data_extaract.txt")

            if 'BEGIN ZONE DATA' and 'END OF ZONE DATA' in index_value.values():
                zone_begin_key=list(index_value.keys())[list(index_value.values()).index('BEGIN ZONE DATA')]
                zone_end_key=list(index_value.keys())[list(index_value.values()).index('END OF ZONE DATA')]
            #     print(bus_begin_key,bus_end_key)
                with open(files_location+'//'+auctionfile, "r") as text_file:
                    

                    for line in itertools.islice(text_file, zone_begin_key+1,zone_end_key):
                        with open(files_location+'//'+"zone_data_extaract.txt", "a") as zone_data_extaract:
                            zone_data_extaract.write(line)
                            
            #extracting BEGIN OWNER DATA  from file 
            if os.path.exists(files_location+'//'+"owner_data_extaract.txt"):
                os.remove(files_location+'//'+"owner_data_extaract.txt")

            if 'BEGIN OWNER DATA' and 'END OF OWNER DATA' in index_value.values():
                owner_begin_key=list(index_value.keys())[list(index_value.values()).index('BEGIN OWNER DATA')]
                owner_end_key=list(index_value.keys())[list(index_value.values()).index('END OF OWNER DATA')]
            #     print(bus_begin_key,bus_end_key)
                with open(files_location+'//'+auctionfile) as text_file:
                    

                    for line in itertools.islice(text_file, owner_begin_key+1,owner_end_key):
                        with open(files_location+'//'+"owner_data_extaract.txt", "a") as owner_data_extaract:
                            owner_data_extaract.write(line)
            # making dataframes and merging 
            bus_columns=['ID','NAME','BASKV','IDE','GL','BL','AREAID','ZONEID','VM','VA','OWNERID']
            df_bus_model=pd.read_csv(files_location+'//'+'bus_data_extaract.txt',header=None,names=bus_columns)
            area_column=['AREAID','AREA']
            df_area=pd.read_csv(files_location+'//'+'area_data_extaract.txt',header=None,usecols=[0,4],names=area_column)
            df_bus_area=df_bus_model.merge(df_area,on='AREAID', how='left')
            zone_column=['ZONEID','ZONE']
            df_zone=pd.read_csv(files_location+'//'+'zone_data_extaract.txt',header=None,names=zone_column)
            df_zone['ZONE']=[value.replace("'",'').strip().split(' ')[0] for value in df_zone['ZONE']]
            df_bus_area_zone=df_bus_area.merge(df_zone,on='ZONEID', how='left')
            owner_column=['OWNERID','OWNER']
            df_owner=pd.read_csv(files_location+'//'+'owner_data_extaract.txt',header=None,names=owner_column)
            df_bus_area_zone_owner=df_bus_area_zone.merge(df_owner,on='OWNERID', how='left')
            gen_column=['ID','GEN','QGEN']
            df_gen=pd.read_csv(files_location+'//'+'gen_data_extaract.txt',header=None,usecols=[0,2,3],names=gen_column)
            # for getting max gen value
            # df_gen["RANK"] = df_gen.groupby("ID")["GEN"].rank(method="first", ascending=False)
            #for getting first value 
            df_gen["RANK"] = df_gen.groupby("ID")["GEN"].rank(method="first", ascending=True)
            df_gen_max = df_gen[df_gen['RANK'] == 1] 
            df_bus_area_zone_owner_gen=df_bus_area_zone_owner.merge(df_gen_max,on='ID',how='left')
            load_column=['ID','LOAD']
            df_load=pd.read_csv(files_location+'//'+'load_data_extract.txt',header=None,usecols=[0,5],names=load_column)
            df_load["load_RANK"] = df_load.groupby("ID")["LOAD"].rank(method="first", ascending=False)
            df_load_max = df_load[df_load['load_RANK'] == 1] 
            df_bus_area_zone_owner_gen_load=df_bus_area_zone_owner_gen.merge(df_load_max,on='ID',how='left')
            df_bus_area_zone_owner_gen_load['NAME']=[value.replace("'",'').strip() for value in df_bus_area_zone_owner_gen_load['NAME']]
            df_bus_area_zone_owner_gen_load['AREA']=[value.replace("'",'').strip() for value in df_bus_area_zone_owner_gen_load['AREA']]
            df_bus_area_zone_owner_gen_load['OWNER']=[value.replace("'",'').strip() for value in df_bus_area_zone_owner_gen_load['OWNER']]
            df_bus_area_zone_owner_gen_load[['GEN', 'QGEN','LOAD']] = df_bus_area_zone_owner_gen_load[['GEN', 'QGEN','LOAD']].fillna(value=0)
            df_bus_area_zone_owner_gen_load['BU_ISO']='PJMISO'
            df_bus_area_zone_owner_gen_load['QLOAD']=df_bus_area_zone_owner_gen_load['BL']
            df_bus_area_zone_owner_gen_load['RELEASE_DATE']=webfiledate
            df_bus_area_zone_owner_gen_load['SFLF']=0
            df_bus_area_zone_owner_gen_load['MODEL_IDX']=0
            df_bus_area_zone_owner_gen_load['INSERTED_DATE']=datetime.now()
            df_bus_area_zone_owner_gen_load['AUCTION_TYPE']=auction_type
            df_final=df_bus_area_zone_owner_gen_load[['BU_ISO','ID','NAME','BASKV','IDE','GL','BL','AREA','ZONE','VM','VA','OWNER','GEN','LOAD','QGEN','QLOAD','RELEASE_DATE','AUCTION_TYPE','MODEL_IDX','INSERTED_DATE','SFLF']]
            # print(df_final)
            snow_upload_csv=f"SNOW_bus_model_{auction_type}.csv"
            # snow_upload_csv_location = os.getcwd() + '\\' + extract_dir_name + '\\'+snow_upload_csv
            # auction_new_record.to_csv(snow_upload_csv, index=False, date_format='%Y-%m-%d', quoting=csv.QUOTE_MINIMAL)
            df_final.to_csv(snow_upload_csv, index=False,  quoting=csv.QUOTE_MINIMAL)
            conn=get_connection()




            # removing earlier stage file from staging area
            conn.cursor().execute('use database POWERDB')
            conn.cursor().execute('use schema POWERDB.ISO')
            conn.cursor().execute('remove @%PJM_BUS_MODEL')

            conn.cursor().execute("PUT file://{} @%PJM_BUS_MODEL overwrite=true".format(snow_upload_csv))
            conn.cursor().execute('''
                    COPY INTO PJM_BUS_MODEL file_format=(type=csv 
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

            # print(f"error while executing put {e}")
            # conn.cursor().execute('rollback')           
            conn.close()
            return df_final
        
            
            
        else:
            
            return "new file not found"
    except Exception as e :
        bumail(['indiapowerit@biourja.com'], 'PJM_BUS_MODEL_FAILED' , 'PJM_BUS_MODEL,ERROR IS - {}'.format(e) )
        bulog(process_name=f'PJM_BUS_MODEL-{auction_type}',status='FAILED',table_name=f'{tablename}', row_count=0, log=json, warehouse=warehouse)
      
        print (f"error found {e} please check for auction type {auction_type}")


def logout():
    browser.close()
    pass

    
if __name__ == "__main__": 
    try:
        #login to pjm website 
        login()
        # getting monthly file from web site
        result=bus_model(monthly,monthly_date_path,monthly_file_path)
        if isinstance(result, pd.DataFrame):
            print(f"new rows inserted for {monthly}  {len(result)}")
            bulog(process_name=f'PJM_BUS_MODEL_{monthly}',status='Completed',table_name=f'{tablename}', row_count=len(result), log=json, warehouse=warehouse)
         
        else:
            bulog(process_name=f'PJM_BUS_MODEL_{monthly}',status='Completed',table_name=f'{tablename}', row_count=0, log=json, warehouse=warehouse)
         
            print(result)
        print("annual process started")
        # getting annual file from the website
        result=bus_model(annual,annual_date_path,annual_file_path)
        if isinstance(result, pd.DataFrame):
            bulog(process_name=f'PJM_BUS_MODEL_{annual}',status='Completed',table_name=f'{tablename}', row_count=len(result), log=json, warehouse=warehouse)
         
            print(f"new rows inserted for {annual}  {len(result)}")
        else:
            bulog(process_name=f'PJM_BUS_MODEL_{annual}',status='Completed',table_name=f'{tablename}', row_count=0, log=json, warehouse=warehouse)
         
            print(result)
        # closing browser
        logout()
        
    except Exception as e:
        bumail(['indiapowerit@biourja.com'], 'PJM_BUS_MODEL_FAILED' , 'PJM_BUS_MODEL,ERROR IS - {}'.format(e) )
        bulog(process_name='PJM_BUS_MODEL',status='FAILED',table_name=f'{tablename}', row_count=0, log=json, warehouse=warehouse)
         
        print(f'process failed {e}')