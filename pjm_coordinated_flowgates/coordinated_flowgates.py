import pandas as pd
import numpy as np
from datetime import date, datetime
import bu_alerts
import bu_snowflake
import bu_config
import logging
from snowflake.connector.pandas_tools import pd_writer
import functools


def get_df(url):
    try:
        logger.info("Inside get_df function")
        df = pd.read_csv(url,encoding= 'unicode_escape')
        logger.info("Dataframe created from the source successfully")
        release_date = df.columns[0].split(':')[1].split(' ')[1]
        logger.info("Release date extracted successfully")
        df.dropna(subset = ['Unnamed: 1','Unnamed: 2','Unnamed: 3'], inplace = True)
        df.columns = ['FLOWGATE_ID','FLOWGATE_NAME','OWNER','FLOWGATE_TYPE']
        df = df[1:]
        df['RELEASE_DATE'] = release_date
        df['INSERT_DATE'] = str(datetime.now())
        df['UPDATE_DATE'] = str(datetime.now())
        logger.info("Final df created successfully")
        print(df)
        return df,release_date
    except Exception as e:
        print(e)
        raise e

def upload_in_sf(tablename, df,release_date):
    logger.info("Inside upload_in_sf function")
    total_rows = 0
    try:
        engine = bu_snowflake.get_engine(
                    database=databasename,
                    role=f"OWNER_{databasename}",    
                    schema= schemaname                           
                )
        conn = engine.connect()
        logger.info("Engine object created successfully")

        check_query = f"select * from {databasename}.{schemaname}.{tablename} where RELEASE_DATE = '{release_date}'"
        check_rows = conn.execute(check_query).fetchall()
        if len(check_rows) > 0:
            logger.info(f"The values are already present for {release_date}")
        else:
            df.to_sql(tablename.lower(), 
                    con=engine,
                    index=False,
                    if_exists='append',
                    schema=schemaname,
                    method=functools.partial(pd_writer, quote_identifiers=False)
                    )
            logger.info(f"Dataframe Inserted into the table {tablename} for release date {release_date}")
            total_rows += len(df)
    except Exception as e:
        logger.exception("Exception while inserting data into snowflake")
        logger.exception(e)
        raise e
    finally:        
        conn.close()      
        engine.dispose()
        logger.info("Engine object disposed successfully and connection object closed")
        return total_rows

if __name__ == '__main__':
    try:
        job_id=np.random.randint(1000000,9999999)
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        logfilename = bu_alerts.add_file_logging(logger,process_name= 'PJM_COORDINATED_FLOWGATES')

        logger.info("Execution started")    
        credential_dict = bu_config.get_config('PJMISO website data','PJM_COORDINATED_FLOWGATES')
        processname = credential_dict['PROJECT_NAME']
        databasename = credential_dict['DATABASE']
        # databasename = 'POWERDB_DEV'
        schemaname = credential_dict['TABLE_SCHEMA']
        tablename = credential_dict['TABLE_NAME']
        url = credential_dict['SOURCE_URL']
        # url = "https://ftp.pjm.com/pub/market/energy/market-to-market/pjm-coordinated-flowgates.csv"
        process_owner = credential_dict['IT_OWNER']
        receiver_email = credential_dict['EMAIL_LIST']
        # receiver_email = "Mrutunjaya.Sahoo@biourja.com,radha.waswani@biourja.com"
        # receiver_email = 'priyanka.solanki@biourja.com'
        logger.info("All the credential details fetched from creential dict")

        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'

        bu_alerts.bulog(process_name=processname,database=databasename,status='Started',table_name=tablename,
            row_count=0, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)

        logger.info("Get df function calling")
        df,release_date = get_df(url)
        logger.info("Get df function completed successfully")

        logger.info("Upload to sf function calling")
        rows = upload_in_sf(tablename, df,release_date)
        logger.info("Upload to sf function completed successfully")
        print("Done")

        bu_alerts.bulog(process_name=processname,database=databasename,status='Completed',table_name=tablename,
            row_count=rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)
        if rows == 0:
            subject = f"JOB SUCCESS - {tablename}  NO new data found"
        else:
            subject = f"JOB SUCCESS - {tablename}  inserted {rows} rows"

        bu_alerts.send_mail(
            receiver_email = receiver_email, 
            mail_subject = subject,
            mail_body=f'{tablename} completed successfully, Attached logs',
            attachment_location = logfilename
        )
    except Exception as e:
        print("Exception caught during execution: ",e)
        logging.exception(f'Exception caught during execution: {e}')
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name= processname,database=databasename,status='Failed',table_name=tablename,
            row_count=rows, log=log_json, warehouse='ITPYTHON_WH',process_owner=process_owner)

        bu_alerts.send_mail(
            receiver_email = receiver_email,
            mail_subject = f'JOB FAILED - {tablename}',
            mail_body=f'{tablename} failed during execution, Attached logs',
            attachment_location = logfilename
        )
    
    