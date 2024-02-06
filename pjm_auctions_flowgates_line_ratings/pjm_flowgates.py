import logging
import os
import sys
from warnings import warn_explicit
import bu_snowflake
import pandas as pd 
import requests
from lxml import html
from datetime import datetime,timedelta
# To get credentials used in process	
from bu_config import get_config
import re
import numpy as np
from bu_snowflake import get_connection
import bu_alerts
from snowflake.connector.pandas_tools import pd_writer
import functools

def get_df_from_source_and_upload():
    try:
        total_rows = 0
        page = requests.get(credential_dict['SOURCE_URL'].split(';')[0])
        tree = html.fromstring(page.content)
        release_date = tree.xpath("/html/body/form/div[4]/div/div/div[2]/div[1]/article/table[2]/tbody/tr[1]/td[2]/div/span")[0].text
        release_date = datetime.strptime(release_date, "%m.%d.%Y")
        max_date = get_max_date(databasename, schemaname, tablename, "RELEASE_DATE")
        print("Release_date is :::::::::",release_date)
        print("Max_date is :::::::::::",max_date)
        print("Compare release_date and max_date.....",release_date > datetime.strptime(str(max_date)[:10], "%Y-%m-%d"))
        if release_date > datetime.strptime(str(max_date)[:10], "%Y-%m-%d"):
            alloc_site = "https://www.pjm.com/-/media/etools/oasis/atc-information/allocs.ashx?la=en"
            r = requests.get(credential_dict['SOURCE_URL'].split(';')[1])
            content = r.content.decode('UTF-8')
            alloc_file = "C:/Temp/alloc.csv"
            with open(alloc_file, "w") as csvfile:
                csvfile.write(content)
            with open(alloc_file, "r") as csvfile:
                dates = csvfile.read()
            dates = dates.split("\n")[0].split(",")
            allocdf = pd.read_csv(alloc_file, header=1)
            cols = list(allocdf.columns)
            for i in range(0, len(cols)):
                if "." in cols[i]:
                    cols[i] = cols[i].split(".")[0] + cols[i].split(".")[1]
            allocdf.columns = cols
            dates_iso = list(zip(dates, cols))[3:]
            os.remove(alloc_file)
            r = requests.get(credential_dict['SOURCE_URL'].split(';')[2])
            content = r.content.decode('UTF-8')
            flowgates_file = "C:/Temp/flowgates.csv"
            with open(flowgates_file, "w", encoding="UTF-8") as csvfile:
                csvfile.write(content)
            flowgatesdf = pd.read_csv(flowgates_file)
            os.remove(flowgates_file)
            flowgatesdf.columns = ["FGID"] + list(flowgatesdf.columns)[1:]

            resdf_cols = ["NERC_FG_ID", "FG_NAME_DESC", "RC", "CA", "TP", "DIRECTION", "FLOW_MONTH", "ISO",
                        "CAPACITY_MW", "MODEL_IDX", "RELEASE_DATE", "INSERT_DATE"]
            resdf = pd.merge(allocdf, flowgatesdf, on="FGID", how="left")
            resdf["Capacity Values"] = resdf.iloc[:, :].apply(list, axis=1)
            vals = list(resdf["Capacity Values"])

            dflist = []
            for val in vals:
                for i in range(len(dates_iso)):    #(0, 30):
                    dflist_item = tuple(val[:1] + val[-4:] + val[2:3] + [dates_iso[i][0]] + [re.sub(r"\d", "", dates_iso[i][1])] + [val[3+i]])
                    dflist.append(dflist_item)
            df = pd.DataFrame(dflist, columns=resdf_cols[:-3])
            
            df['MODEL_IDX'] = 0
            df["INSERT_DATE"] = str(datetime.now())
            df["UPDATE_DATE"] = str(datetime.now())
            df["RELEASE_DATE"] = str(release_date)
            df['FLOW_MONTH'] = pd.to_datetime(df['FLOW_MONTH']).dt.strftime('%Y-%m-%d')
            flow_months_list = df['FLOW_MONTH'].unique().tolist()
            flow_months_list.sort()
            conn = get_connection(role=role,database=databasename,schema=schemaname)
            for month in flow_months_list: 
                conn.cursor().execute("use warehouse quant_wh")
                # CHANGE VERSION 0 to -1
                conn.cursor().execute("update {}.{}.{} set model_idx = model_idx-1 \
                    where FLOW_MONTH = '{}'".format(databasename, schemaname, tablename,month))
            conn.close()

            total_rows += load_df_to_sf(df, databasename, schemaname, tablename, release_date)     
    except Exception as e:
        logger.exception(e)
        raise e
    return total_rows

def get_max_date(databasename, schemaname, tablename, datecolumn):

    """
    Retrieves the most recent date in the table.

    Args:
        database : the database in question
        schemename : the schema in question
        tablename : the table in question
        datecolumn : the column corresponding to the date

    Returns:
        max_date : the most recent date in the table
        or
        None : if the table is empty
    """

    sql = '''
    select max({}) as MAX_DATE
    from {}
    '''.format(datecolumn, databasename+"."+schemaname+"."+tablename)

    try:
        conn = conn = get_connection(role='OWNER_{}'.format(databasename),database=databasename,schema=schemaname)
        conn.cursor().execute("USE WAREHOUSE QUANT_WH")

        cs = conn.cursor()
        cs.execute(sql)

        df = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])
        conn.close()
        
        return df["MAX_DATE"].values[0]

    except Exception as e:
        print(f"Exception caught {e} during execution")
        # logger.exception(f'Exception caught during execution: {e}')
        raise e


def load_df_to_sf(df, databasename, schemaname, tablename, release_date):

    """
        Takes dataframe and a date as the argument. The dataframe is uploaded in snowflake on the basis of the date which is RELEASE_DATE
        in this case. First checks the data in snowflake on the basis of RELEASE_DATE, if data is available then deletes the data for 
        that day and inserts again otherwise uploads the data directly.

        Params
        ------
        df: Dataframe
            The dataframe to be uploaded in the snowflake.
        release_date: Date
            The value of the RELEASE_DATE on the basis of which the data is uploaded in snowflake.

        Returns:
        --------
        total_rows: int
            Total number of rows inserted for the dataframe.
    """
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

        check_query = f"select * from {databasename}.{schemaname}.{tablename} where release_date = '{release_date}'"
        check_rows = conn.execute(check_query).fetchall()
        if len(check_rows) > 0:
            logger.info(f"The values are already present for {release_date}")
            del_data = conn.execute(f"""delete from {databasename}.{schemaname}.{tablename} where release_date = '{release_date}'""").fetchall()
            logger.info(f"{del_data[0][0]} number of rows deleted as they were already present for date {release_date} ")
        else:
            logger.info(f"NO values are present for {release_date}")
        df.to_sql(tablename.lower(), 
                con=engine,
                index=False,
                if_exists='append',
                schema=schemaname,
                method=functools.partial(pd_writer, quote_identifiers=False)
                )
        logger.info(f"Dataframe Inserted into the table {tablename} for RELEASE_DATE {release_date} and total rows are {len(df)}")
        total_rows += len(df)
        return total_rows
    except Exception as e:
        logger.exception("Exception while inserting data into snowflake")
        logger.exception(e)
        raise e


if __name__ == "__main__": 
    job_id=np.random.randint(1000000,9999999)
    starttime=datetime.now()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    log_file_location = bu_alerts.add_file_logging(logger,process_name= 'pjmiso_flowgates')

    logger.info('Execution Started')
    rows=0
    try:
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        logger.info('Start work at {} ...'.format(starttime.strftime('%Y-%m-%d %H:%M:%S')))  
        credential_dict = get_config('PJMISO website data','PJM_FLOWGATES')
        databasename = credential_dict['DATABASE']
        # databasename = "POWERDB_DEV"
        role = f"OWNER_{databasename}"
        schemaname = credential_dict['TABLE_SCHEMA']
        tablename = credential_dict['TABLE_NAME']
        receiver_email = credential_dict['EMAIL_LIST']
        # receiver_email = "mrutunjaya.sahoo@biourja.com"
        # receiver_email = "priyanka.solanki@biourja.com"
        print(tablename)
        bu_alerts.bulog(process_name=tablename,
            database=databasename,
            status='Started',
            table_name=databasename +'.'+ schemaname +'.'+ tablename, 
            row_count=rows, 
            log=log_json, 
            warehouse='QUANT_WH',
            process_owner=credential_dict['IT_OWNER'])

        rows = 0
        rows += get_df_from_source_and_upload()

        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name=tablename,
            database=databasename,
            status='Completed',
            table_name = databasename +'.'+ schemaname +'.'+ tablename, 
            row_count=rows, log=log_json,
            warehouse='QUANT_WH',
            process_owner=credential_dict['IT_OWNER']) 

        logger.info('Execution Done')
        bu_alerts.send_mail(
            receiver_email = receiver_email,
            mail_subject ='JOB SUCCESS - {} and rows uploaded {}'.format(tablename, rows),
            mail_body = '{} completed successfully, Attached logs'.format(tablename),
            attachment_location = log_file_location
        )   
    except Exception as e:     
        print("Exception caught during execution: ",e)
        logger.exception(f'Exception caught during execution: {e}')
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name=tablename,
            database=databasename,
            status='Failed',
            table_name = databasename +'.'+ schemaname +'.'+ tablename,
            row_count=0, 
            log=log_json, 
            warehouse='QUANT_WH',
            process_owner=credential_dict['IT_OWNER']) 
        bu_alerts.send_mail(
            receiver_email = receiver_email,
            mail_subject='JOB FAILED - {}'.format(tablename),
            mail_body='{} failed during execution, Attached logs'.format(tablename),
            attachment_location = log_file_location
        )
    
    endtime=datetime.now()
    print('Complete work at {} ...'.format(endtime.strftime('%Y-%m-%d %H:%M:%S')))
    print('Total time taken: {} seconds'.format((endtime-starttime).total_seconds()))
    

# %%
