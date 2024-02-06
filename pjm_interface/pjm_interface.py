import pandas as pd
import datetime
import logging
import sys, pathlib
sys.path.append(pathlib.Path().absolute().__str__())
from timeit import default_timer as timer
from bu_snowflake import get_engine
import bu_alerts
# To get credentials used in process	
from bu_config import get_config
from snowflake.connector.pandas_tools import pd_writer
import functools
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
import os
import time

iso_name = 'PJMISO'

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

def get_data_from_link(auction_type: str)-> tuple:
    logger.info(f'Running for {auction_type} auction.')
    if auction_type=='Annual':
        link = credential_dict['SOURCE_URL'].split(';')[0]
    else:
        link = credential_dict['SOURCE_URL'].split(';')[1]

    files_location = os.getcwd() + "\\pjm_interface\\download_pjm_interface"
    file_name = link.split('/')[-1]
    logger.info(f"File name extracted: {file_name}")

    
    logging.info("Calling remove_existing_files function")
    remove_existing_files(files_location)
    logging.info("Remove existing files completed successfully")
    
    options = Options()
    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.manager.showWhenStarting", False)
    options.set_preference("browser.download.dir", files_location)
    browser = webdriver.Firefox(options=options, executable_path='geckodriver.exe')
    logger.info(f"Browser object created")
    browser.get(link)
    time.sleep(30)
    browser.find_element(By.ID,"idToken1").send_keys(credential_dict['USERNAME'])
    browser.find_element(By.ID,"idToken2").send_keys(credential_dict['PASSWORD'])
    browser.find_element(By.ID,"loginButton_0").click()
    time.sleep(10)
    logger.info(f"File download from browser")
    browser.quit()
    logger.info("Browser object quit successfully")

    if len(os.listdir(files_location)) == 0:
            raise Exception("File could not be downloaded from browser due to website issue")
    
    df = pd.read_csv(files_location + '\\' + file_name)
    logger.info("Dataframe read from csv file")
    os.remove(files_location + '\\' + file_name)
    logger.info(f"File removed after reading")
    release_date = datetime.datetime.strptime(df.columns[0].split(' ')[-1],'%Y%m%d').date()
    logger.info(f'Retrieved data for {auction_type} auction.')
    df = df.iloc[2:]
    interface_def = df[df.columns[:-2]].dropna().reset_index(drop=True)
    interface_limits = df[df.columns[-2:]].dropna().reset_index(drop=True)
    interface_def.columns = ['INTERFACE_NAME','BRANCH_ID_1','BRANCH_ID_2','BRANCH_ID_3','FROM_BUS','TO_BUS','DIRECTION']
    interface_limits.columns = ['INTERFACE_NAME','LIMITS_MW']
    interface_def['BU_ISO'] = iso_name
    interface_def['AUCTION_TYPE'] = auction_type
    interface_def['MODEL_IDX'] = 0
    interface_def['RELEASE_DATE'] = release_date
    interface_def['INSERT_DATE'] = today
    interface_def['UPDATE_DATE'] = today
    interface_limits['BU_ISO'] = iso_name
    interface_limits['AUCTION_TYPE'] = auction_type
    interface_limits['MODEL_IDX'] = 0
    interface_limits['RELEASE_DATE'] = release_date
    interface_limits['INSERT_DATE'] = today
    interface_limits['UPDATE_DATE'] = today
    logger.info(f'Processed the data for {auction_type} auction.')
    return (interface_def,interface_limits,release_date)


def send_to_db(interface_def: pd.DataFrame, interface_limits: pd.DataFrame, release_date,auction_type):
    row_count_limits, row_count_def = (0,0)
    with engine.connect() as conxn:
        existing_data_def = pd.read_sql(
            f"""
                SELECT RELEASE_DATE FROM {DATABASE}.{SCHEMA}.PJM_INTERFACE_DEF
                WHERE MODEL_IDX = 0
                AND AUCTION_TYPE = '{auction_type}'
            """,con=conxn
        )
        
        if len(existing_data_def) > 0:
            existing_release_date = str(existing_data_def['release_date'][0])
            if existing_release_date == str(release_date):
                logger.info(f'Data already up to date for {auction_type} auction.')
            else:
                conxn.execute(f"""
                    DELETE FROM {DATABASE}.{SCHEMA}.PJM_INTERFACE_DEF
                    WHERE RELEASE_DATE='{release_date}'
                    AND AUCTION_TYPE='{auction_type}'
                """)
                conxn.execute(
                    f"""UPDATE {DATABASE}.{SCHEMA}.PJM_INTERFACE_DEF
                    SET MODEL_IDX = -1 
                    WHERE AUCTION_TYPE = '{auction_type}'
                    AND RELEASE_DATE = '{existing_release_date}'
                    """
                )
                logger.info('Updated PJM_INTERFACE_DEF table with -1 values.')
                interface_def.to_sql('PJM_INTERFACE_DEF'.lower(),conxn,index=False,if_exists='append',
                    method=functools.partial(pd_writer, quote_identifiers=False))
                logger.info(f'Inserted {len(interface_def)} rows in PJM_INTERFACE_DEF table.')
                row_count_def = len(interface_def)
        else:
            conxn.execute(f"""
                DELETE FROM {DATABASE}.{SCHEMA}.PJM_INTERFACE_DEF
                WHERE RELEASE_DATE='{release_date}'
                AND AUCTION_TYPE='{auction_type}'
            """).fetchone()
            interface_def.to_sql('PJM_INTERFACE_DEF'.lower(),conxn,index=False,if_exists='append',
                    method=functools.partial(pd_writer, quote_identifiers=False))
            logger.info(f'Inserted {len(interface_def)} rows in PJM_INTERFACE_DEF table.')
            row_count_def = len(interface_def)
            
        existing_data_limits = pd.read_sql(
            f"""
                SELECT RELEASE_DATE FROM {DATABASE}.{SCHEMA}.PJM_INTERFACE_LIMITS
                WHERE MODEL_IDX = 0
                AND AUCTION_TYPE = '{auction_type}'
            """,con=conxn
        )

        if len(existing_data_limits) >0:
            existing_release_date = str(existing_data_limits['release_date'][0])
            if existing_release_date == str(release_date):
                logger.info(f'Data already up to date for {auction_type} auction.')
            else:
                conxn.execute(f"""
                    DELETE FROM {DATABASE}.{SCHEMA}.PJM_INTERFACE_LIMITS
                    WHERE RELEASE_DATE='{release_date}'
                    AND AUCTION_TYPE='{auction_type}'
                """)
                conxn.execute(
                    f"""UPDATE {DATABASE}.{SCHEMA}.PJM_INTERFACE_LIMITS
                    SET MODEL_IDX = -1 
                    WHERE AUCTION_TYPE = '{auction_type}'
                    AND RELEASE_DATE = '{existing_release_date}'
                    """
                )
                logger.info('Updated PJM_INTERFACE_LIMITS table with -1 values.')
                interface_limits.to_sql('PJM_INTERFACE_LIMITS'.lower(),conxn,index=False,if_exists='append',
                    method=functools.partial(pd_writer, quote_identifiers=False))
                logger.info(f'Inserted {len(interface_limits)} rows in PJM_INTERFACE_LIMITS table.')
                row_count_limits = len(interface_limits)
        else:
            conxn.execute(f"""
                DELETE FROM {DATABASE}.{SCHEMA}.PJM_INTERFACE_LIMITS
                WHERE RELEASE_DATE='{release_date}'
                AND AUCTION_TYPE='{auction_type}'
            """)
            interface_limits.to_sql('PJM_INTERFACE_LIMITS'.lower(),conxn,index=False,if_exists='append',
                    method=functools.partial(pd_writer, quote_identifiers=False))
            logger.info(f'Inserted {len(interface_limits)} rows in PJM_INTERFACE_LIMITS table.')
            row_count_limits = len(interface_limits)
    
    return (row_count_def,row_count_limits)
                
def main():
    """Main funciton of the process.
    """

    logger.info('Execution started.')
    (interface_def_monthly, interface_limits_monthly,monthly_release_date) = get_data_from_link(auction_type='Monthly')
    (interface_def_annual, interface_limits_annual,annual_release_date) = get_data_from_link(auction_type='Annual')
    (row_count_def_monthly,row_count_limits_monthly) = send_to_db(interface_def_monthly,interface_limits_monthly,monthly_release_date,'Monthly')
    (row_count_def_annual,row_count_limits_annual) = send_to_db(interface_def_annual,interface_limits_annual,annual_release_date,'Annual')
    return (row_count_def_monthly+row_count_def_annual,row_count_limits_monthly+row_count_limits_annual)


if __name__ == "__main__":
    start = timer()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logfilename = bu_alerts.add_file_logging(logger,process_name='pjm_interface')
    tablenames = ['PJM_INTERFACE_DEF','PJM_INTERFACE_LIMITS']
    try:
        credential_dict = get_config('PJMISO website data','PJM_INTERFACE_DEF')
        DATABASE = credential_dict['DATABASE']
        # DATABASE = "POWERDB_DEV"
        SCHEMA = credential_dict['TABLE_SCHEMA']
        tablename = credential_dict['TABLE_NAME']
        logger.info(f'Execution started')
        today = str(datetime.datetime.now())
        engine = get_engine(role=f'OWNER_{DATABASE}',database=DATABASE,schema=SCHEMA)
        for tablename in tablenames:
            bu_alerts.bulog(
                process_name=tablename,
                status='started',
                table_name=tablename,
                process_owner=credential_dict['IT_OWNER']

            )
        row_counts = main()
        end = timer()
        print(f'Executed in {end-start} seconds.')
        logger.info(f'Executed in {end-start} seconds.')
        for index,tbl in enumerate(tablenames):
            bu_alerts.bulog(
                process_name=tbl,
                status='completed',
                table_name=tbl,
                process_owner=credential_dict['IT_OWNER'],
                row_count= row_counts[index]
            )
            bu_alerts.send_mail(
                receiver_email=credential_dict['EMAIL_LIST'],
                mail_subject="Job Success - {}".format(tbl),
                mail_body=f"Process completed successfully, Attached logs",
                attachment_location=logfilename
            )
    except Exception as e:
        print(e)
        logger.exception(f'Error occuered. {e}')
        for tablename in tablenames:
            bu_alerts.bulog(
                process_name=tablename,
                status='failed',
                table_name=tablename,
                process_owner=credential_dict['IT_OWNER']
            )
            bu_alerts.send_mail(
                receiver_email=credential_dict['EMAIL_LIST'],
                mail_subject='JOB FAILED - {}  {}'.format(tablename,e),
                mail_body=f"Process failed, Attached logs",
                attachment_location=logfilename
            )
    finally:
        engine.dispose()