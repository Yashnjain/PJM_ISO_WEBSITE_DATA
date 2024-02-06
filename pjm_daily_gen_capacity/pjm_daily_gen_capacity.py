import bu_alerts
import logging
import datetime
import requests
import pandas as pd
import bu_config
import bu_snowflake
import urllib.parse
from dateutil.relativedelta import relativedelta
from snowflake.connector.pandas_tools import write_pandas

#Api constraints for PJM Daily gen are
#Can only query for 366 days window, esentially an year data
#Total rows existing for the window are unknown so query once to get that and then query total data by changing params

def extract_from_api(credential_dict: dict,bid_datetime_begin_string: str)->tuple:
    """Extraction of data from PJM API

    Args:
        credential_dict (dict): Credentials loaded from config params
        bid_datetime_begin_string (str): Time window to capture data for

    Returns:
        tuple: Tuple of (totalRows,data_df)
    """
    try:
        totalRows,data_df = (0,pd.DataFrame())
        headers = {
            # Request headers
            'Ocp-Apim-Subscription-Key': credential_dict['API_KEY'],
        }
        param_dict = {
                        # Request parameters
                        'download': False,
                        'rowCount': 10000,
                        'startRow': 1,
                        'bid_datetime_beginning_ept': bid_datetime_begin_string,
                    }
        params = urllib.parse.urlencode(param_dict)
        res = requests.get('https://api.pjm.com/api/v1/day_gen_capacity?',params=params,headers=headers)
        if res.status_code != 200:
            logger.error(f'Invalid request made. Received status code {res.status_code}')
        else:
            totalRows = res.json()['totalRows']
            data_df = pd.DataFrame(res.json()['items'])
            logger.info(f'Successfully fetched data between time duration {bid_datetime_begin_string} and received {totalRows} rows.')
    except Exception as e:
        logger.exception(f'Something went wrong during API call. More Details {e}')
    return (totalRows, data_df)


def get_data(credential_dict: dict)->pd.DataFrame:
    """Func to get the data and tranform it.

    Args:
        credential_dict (dict): Credentials loaded from config params

    Returns:
        pd.DataFrame: Result dataframe of extracted data, to be loaded in database
    """
    df_cols = ['BID_DATETIME_BEGINNING_UTC', 'BID_DATETIME_BEGINNING_EPT',
       'ECONOMIC_MAX_MW', 'EMERGENCY_MAX_MW', 'TOTAL_COMMITTED_MW',
       'INSERT_DATE', 'UPDATE_DATE']
    data_df = pd.DataFrame(columns=df_cols)
    try:
        data_df_list = []
        totalRows = 1
        with conn.cursor() as cursor:
            existing_df = cursor.execute(f'select * from {DATABASE}.{SCHEMA}.{tablename} ORDER BY BID_DATETIME_BEGINNING_EPT DESC LIMIT 1').fetch_pandas_all()
        if existing_df.empty:
            start_datetime = datetime.datetime(2012, 1, 1, 0, 0)
        else:
            start_datetime = existing_df['BID_DATETIME_BEGINNING_EPT'][0].to_pydatetime()
            start_datetime = start_datetime + datetime.timedelta(hours=1)
        while totalRows != 0:
            end_datetime = start_datetime + relativedelta(years=1)
            bid_datetime_begin_string = f"{start_datetime.strftime('%m/%d/%Y %H:00')} to {end_datetime.strftime('%m/%d/%Y %H:00')}"
            (totalRows, data_df) = extract_from_api(credential_dict,bid_datetime_begin_string)
            start_datetime = end_datetime
            if not data_df.empty:
                data_df['insert_date'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%S:%M')
                data_df['update_date'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%S:%M')
                data_df.columns = df_cols
                data_df['BID_DATETIME_BEGINNING_EPT'] = pd.to_datetime(data_df['BID_DATETIME_BEGINNING_EPT']).astype(str)
                data_df['BID_DATETIME_BEGINNING_UTC'] = pd.to_datetime(data_df['BID_DATETIME_BEGINNING_UTC']).astype(str)
            else:
                data_df = pd.DataFrame(columns=df_cols)
            data_df_list.append(data_df)    
        data_df = pd.concat(data_df_list)
        data_df = data_df.drop_duplicates(subset=['BID_DATETIME_BEGINNING_EPT','BID_DATETIME_BEGINNING_UTC']).reset_index(drop=True)
    except Exception as e:
        logger.exception(f'Something went wrong during data fetch. More Details {e}')
    return data_df

def load_data_to_table(data_df: pd.DataFrame)->int:
    """Func to load data into table

    Args:
        data_df (pd.DataFrame): Extracted data in form of dataframe

    Returns:
        int: Number of rows loaded into database.
    """
    try:
        nrows = 0
        if data_df.empty:
            logger.info('No new data found.')
            return nrows
        success, nchunks, nrows, _ = write_pandas(
                            conn, 
                            data_df,
                            database=DATABASE,
                            schema=SCHEMA, 
                            table_name=tablename,
                            quote_identifiers=False,
                            chunk_size=100000
                        )
        if success:
            logger.info(f'Added {nrows} in {DATABASE}.{SCHEMA}.{tablename}')
        else:
            logger.info(f'No rows to add.')
    except Exception as e:
        logger.exception(f'Issue during data upload. More Details {e}')
    return nrows


if __name__ == "__main__":
    logging.getLogger('snowflake.connector').setLevel(logging.ERROR)
    logging.getLogger('azure.core').setLevel(logging.ERROR)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logfilename = bu_alerts.add_file_logging(logger,process_name='pjm_daily_gen_capacity')
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    try:
        credential_dict = bu_config.get_config('PJMISO website data','PJM_DAILY_GENERATION_CAPACITY')
        DATABASE = credential_dict['DATABASE']
        SCHEMA = credential_dict['TABLE_SCHEMA']
        tablename = credential_dict['TABLE_NAME']
        logger.info(f'Execution started')
        today = str(datetime.datetime.now())
        conn = bu_snowflake.get_connection(role=f'OWNER_{DATABASE}',database=DATABASE,schema=SCHEMA)
        bu_alerts.bulog(
            process_name=tablename,
            status='started',
            table_name=tablename,
            process_owner=credential_dict['IT_OWNER']

        )
        data_df = get_data(credential_dict)
        row_count = load_data_to_table(data_df)
        bu_alerts.bulog(
            process_name='pjm_daily_gen_capacity',
            status='completed',
            table_name=tablename,
            process_owner=credential_dict['IT_OWNER'],
            row_count= row_count
        )
        bu_alerts.send_mail(
            # receiver_email='priyanka.solanki@biourja.com',
            receiver_email=credential_dict['EMAIL_LIST'],
            mail_subject=f"Job Success - {tablename}",
            mail_body=f"Process completed successfully, Attached logs",
            attachment_location=logfilename
        )
    except Exception as e:
        logger.exception(f'Error occuered. {e}')
        bu_alerts.bulog(
            process_name=tablename,
            status='failed',
            table_name=tablename,
            process_owner=credential_dict['IT_OWNER']
        )
        bu_alerts.send_mail(
            receiver_email=credential_dict['EMAIL_LIST'],
            mail_subject=f"Job Failed - {tablename}",
            mail_body=f"Process failed, Attached logs",
            attachment_location=logfilename
        )
    finally:
        conn.close()