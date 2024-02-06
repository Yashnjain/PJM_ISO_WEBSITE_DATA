#%%
import logging
from bu_snowflake import get_connection
import datetime
import pandas as pd
import re
import os
import email.message
import smtplib
import numpy as np

#%%

def get_table_columns(databasename, schemaname, tablename):

    """
    Retrieves a list of the column headers of the table.

    Args:
        database : the database in question
        schemename : the schema in question
        tablename : the table in question

    Returns:
        cols_in_db : the list of the column headers of the table
        or
        [] : if error arises when retrieving columns or table doesn't exist
    """

    # SQL query that will return first row of data
    sql = '''
    select *
    from {}
    limit 1
    '''.format(databasename+"."+schemaname+"."+tablename)

    try:
        # Connect to Snowflake
        conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='QUANT')
        conn.cursor().execute("USE WAREHOUSE QUANT_WH")
        cs = conn.cursor()
        cs.execute(sql)

        # Get column names
        df = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])
        conn.close()
        cols_in_db = [x.upper() for x in df.columns]

        return cols_in_db
    except Exception as e:
        print(f"Exception caught {e} during execution")
        # logging.exception(f'Exception caught during execution: {e}')
        raise e

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
        # logging.exception(f'Exception caught during execution: {e}')
        raise e
def get_max_datetime(databasename, schemaname, tablename, datecolumn):

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
        conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='QUANT')
        conn.cursor().execute("USE WAREHOUSE QUANT_WH")

        cs = conn.cursor()
        cs.execute(sql)

        df = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])
        conn.close()
        
        max_date = df["MAX_DATE"].max()
        return max_date if max_date is not np.nan else None

    except Exception as e:
        print(f"Exception caught {e} during execution")
        # logging.exception(f'Exception caught during execution: {e}')
        raise e

def get_max_datetime_ice(databasename, schemaname, tablename, datecolumn, commodity):

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
    from {} where COMMODITY = '{}'
    '''.format(datecolumn, databasename+"."+schemaname+"."+tablename, commodity)
    print(sql)

    try:
        conn = get_connection(role='OWNER_POWERDB_DEV',database='POWERDB_DEV',schema='PMACRO')
        conn.cursor().execute("USE WAREHOUSE QUANT_WH")

        cs = conn.cursor()
        cs.execute(sql)

        df = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])
        conn.close()
        max_date = df["MAX_DATE"].max()
        return max_date if max_date is not np.nan else None

    except Exception as e:
        print(f"Exception caught {e} during execution")
        logging.exception(f'Exception caught during execution: {e}')
        raise e

def get_min_datetime(databasename, schemaname, tablename, datecolumn):

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
    select min({}) as MIN_DATE
    from {}
    '''.format(datecolumn, databasename+"."+schemaname+"."+tablename)

    try:
        conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='QUANT')
        conn.cursor().execute("USE WAREHOUSE QUANT_WH")

        cs = conn.cursor()
        cs.execute(sql)

        df = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])
        conn.close()
        
        min_date = df["MIN_DATE"].min()
        return min_date if min_date is not np.nan else None

    except Exception as e:
        print(f"Exception caught {e} during execution")
        # logging.exception(f'Exception caught during execution: {e}')
        raise e

def get_primary_keys(databasename,schemaname,tablename):
    try:
        primary_key_list = []
        table = databasename + '.' + schemaname + '.' + tablename
        query_primary_key = f'''SHOW PRIMARY KEYS IN {table}'''
        conn = get_connection(role='OWNER_{}'.format(databasename),database=databasename,schema=schemaname)
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

def get_table_pk(databasename, schemaname, tablename):

    """
    Gets a list of primary keys of the table.

    Args:
        databasename : the databse in question
        schemename : the schema in question
        tablename : the table in question

    Returns:
        pks : list of primary keys of the table
    """

    # SQL query to get ddl of table
    sql = '''
    select get_ddl('table', '{}')
    '''.format(databasename+'.'+schemaname+'.'+tablename)

    try:
        # Connect to Snowflake
        conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='QUANT')
        conn.cursor().execute('USE WAREHOUSE QUANT_WH')
        cs = conn.cursor()
        cs.execute(sql)

        # Gets the ddl string
        df = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])
        conn.close()

        # Gets value of the ddl string
        info = df["GET_DDL('TABLE', '{}')".format(databasename+'.'+schemaname+'.'+tablename)].values[0]

        # Finds the part of the string with the primary keys
        substr = info.split("primary key")[1]

        # Returns the list of primary keys
        pks = re.search(r'\(([^)]+)', substr).group(1).split(", ")
        return pks
    except Exception as e:
        print(f"Exception caught {e} during execution")
        # logging.exception(f'Exception caught during execution: {e}')
        raise e

def load_df_to_sf(df, databasename, schemaname, tablename, pklist, truncate = False, uploadinsert = True):

    """
    Saves dataframe as csv and uploads to Snowflake.

    Args:
        df : the dataframe in question
        databasename : the database of the destination in Snowflake
        schemaname : the schema of the destination in Snowflake
        tablename : the table of the destination in Snowflake
        pklist : list of primary keys for the table (used if table does not already have pks)
        truncate (optional) : defaults to not truncating the table prior to data upload
        uploadinsert (optional) : defaults to adding insert_date and update_date columns if not already there

    Returns:
        length of the dataframe : the dataframe was successfully loaded to Snowflake
        or
        None : wherein the columns of the dataframe and the table in Snowflake
               do not share the same columns
    """
    try:
        # Change column headers to all caps and remove whitespace
        df.columns = [re.sub(r"\W", "_", x.upper()) for x in df.columns]

        if uploadinsert:
            df["INSERT_DATE"] = datetime.datetime.now()
            df["UPDATE_DATE"] = datetime.datetime.now()
        
        # Everything looks good to proceed, save as csv file
        csv_file = r"C:/temp/to_snowflake_{}.csv".format(tablename)
        df.to_csv(csv_file, index=False, date_format="%Y-%m-%d %H:%M:%S")
        
        #shutil.copy(csv_file, r"c:/Temp/snowflake{}.csv".format(np.random.randint(10000, 99999)))

        # # Run DDL statement, create table if not exists
        conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='QUANT')
        conn.cursor().execute("USE WAREHOUSE QUANT_WH")
        conn.cursor().execute("USE ROLE OWNER_{}".format(databasename))
        conn.cursor().execute("USE DATABASE {}".format(databasename))
        conn.cursor().execute("USE SCHEMA {}".format(schemaname))
        conn.cursor().execute('remove @%{}'.format(tablename))
        conn.cursor().execute("PUT file://{} @%{} overwrite=true".format(csv_file, tablename))
        conn.cursor().execute('''
            COPY INTO {} file_format=(type=csv
            skip_header=1 field_optionally_enclosed_by = '"' empty_field_as_null=true escape_unenclosed_field=None)
            '''.format(tablename))
        conn.close()

        print("Uploaded.")

        # Remove csv file from local drive
        os.remove(csv_file)

        return len(df)
    except Exception as e:
        print(f"Exception caught {e} during execution")
        logging.exception(f'Exception caught during execution: {e}')
        raise e

def sendmail(to_addr, subject, msgbody):
    """Sends email to list of recipients.

    Args:
        to_addr : the list of recipients
        subject : the subject of the email
        msgbody : contents of the email

    Returns:
        void : has action of sending email
    """
    
    msg = email.message.Message()
    msg['Subject'] = subject
    msg['From'] = 'ftr-dev@biourja.com'

    msg.add_header('Content-Type', 'text/html')

    msg.set_payload(msgbody)

    s = smtplib.SMTP(host='us-smtp-outbound-1.mimecast.com', port=587)
    s.starttls()
    s.login('virtual-out@biourja.com', 't?%;`p39&Pv[L<6Y^cz$z2bn')
    s.sendmail(msg['From'], to_addr, msg.as_string())
    s.quit()

def sendmailtable(df, to_addr, mailsubj):

    msgbody = '<table style="border-spacing: 5px">'    
    msgbody = msgbody + '<tr>'

    for x in list(df.columns):
        msgbody = msgbody + '<td style="font-weight: bold; text-align:left; background-color: light grey">{}</td>'.format(x)
    msgbody = msgbody + '</tr>'

    for i, x in df.iterrows():
        msgbody = msgbody + '<tr>'
        for y in list(df.columns):
            msgbody = msgbody + '<td style="text-align:left">{}</td>'.format(x[y])
        msgbody = msgbody + '</tr>'

    msgbody = msgbody + '</table>'
    sendmail(to_addr, mailsubj, msgbody)


def load_df_to_sf_nex(df, databasename, schemaname, tablename, pklist, truncate):

    """
    Saves dataframe as csv and uploads to Snowflake.

    Args:
        df : the dataframe in question
        databasename : the database of the destination in Snowflake
        schemaname : the schema of the destination in Snowflake
        tablename : the table of the destination in Snowflake
        pklist : list of primary keys for the table (used if table does not already have pks)
        truncate (optional) : defaults to not truncating the table prior to data upload
        uploadinsert (optional) : defaults to adding insert_date and update_date columns if not already there

    Returns:
        length of the dataframe : the dataframe was successfully loaded to Snowflake
        or
        None : wherein the columns of the dataframe and the table in Snowflake
               do not share the same columns
    """
    try:
        # Change column headers to all caps and remove whitespace
        # df.columns = [re.sub(r"\W", "_", x.upper()) for x in df.columns]

        # if uploadinsert:
            # df["INSERT_DATE"] = datetime.datetime.now()
            # df["UPDATE_DATE"] = datetime.datetime.now()

        # Everything looks good to proceed, save as csv file
        csv_file = r"C:/temp/to_snowflake_{}.csv".format(tablename)
        df.to_csv(csv_file, index=False, date_format="%Y-%m-%d %H:%M:%S")
        
        #shutil.copy(csv_file, r"c:/Temp/snowflake{}.csv".format(np.random.randint(10000, 99999)))
        
        conn = get_connection(role='OWNER_{}'.format(databasename),database=databasename,schema=schemaname)
        conn.cursor().execute("USE WAREHOUSE QUANT_WH")
        conn.cursor().execute("USE ROLE OWNER_{}".format(databasename))
        conn.cursor().execute("USE DATABASE {}".format(databasename))
        conn.cursor().execute("USE SCHEMA {}".format(schemaname))
        if truncate:
            conn.cursor().execute("truncate table {}".format(tablename))
        conn.cursor().execute('remove @%{}'.format(tablename))
        conn.cursor().execute("PUT file://{} @%{} overwrite=true".format(csv_file, tablename))
        conn.cursor().execute('''
            COPY INTO {} file_format=(type=csv
            skip_header=1 field_optionally_enclosed_by = '"' empty_field_as_null=true escape_unenclosed_field=None)
            '''.format(tablename))
        conn.close()

        print("Uploaded.")

        # Remove csv file from local drive
        # os.remove(csv_file)

        return len(df)
    except Exception as e:
        print(f"Exception caught {e} in load_df_to_sf")
        logging.exception(f'Exception caught in load_df_to_sf: {e}')
        raise e


#%%