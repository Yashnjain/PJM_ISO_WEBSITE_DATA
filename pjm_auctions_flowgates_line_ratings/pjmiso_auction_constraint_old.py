import sys
import logging
import pathlib
import requests
from lxml import html
import pandas as pd
import numpy as np
import os
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from tenacity import Retrying, RetryError, stop_after_attempt, wait_fixed
# %%
primary_key_list = []
databasename = "POWERDB"
schemaname = "ISO"
tablename = "PJMISO_AUCTION_CONSTRAINT"
# %%


def get_max_date(auction_type, databasename, schemaname, tablename):

    try:
        conn = get_connection()
        cs = conn.cursor()
        cs.execute("use warehouse quant_wh")
        cs.execute("use database {}".format(databasename))
        cs.execute("use schema {}".format(schemaname))
        cs.execute("select max(release_date) as MAX_DATE from {} where auction_type = '{}'".format(
            tablename, auction_type))
        df = pd.DataFrame.from_records(
            iter(cs), columns=[x[0] for x in cs.description])
        conn.close()
        return df["MAX_DATE"].max() if df["MAX_DATE"].max() is not np.nan else None

    except Exception as e:
        logger.exception(e)
        return None


def get_auction_id(auction_name):

    try:
        conn = get_connection()
        cs = conn.cursor()
        cs.execute("use warehouse quant_wh")
        cs.execute("use database powerdb")
        cs.execute("use schema pquant")
        cs.execute("select yes_auction_id from bu_ftr_auction_vw where auction_name = '{}'".format(
            auction_name))
        df = pd.DataFrame.from_records(
            iter(cs), columns=[x[0] for x in cs.description])
        conn.close()
        return df["YES_AUCTION_ID"].values[0]

    except Exception as e:
        logger.exception(e)
        return None


def get_auction_round(auction_name):

    try:
        conn = get_connection()
        cs = conn.cursor()
        cs.execute("use warehouse quant_wh")
        cs.execute("use database powerdb")
        cs.execute("use schema pquant")
        cs.execute("select auction_round from bu_ftr_auction_vw where auction_name = '{}'".format(
            auction_name))
        df = pd.DataFrame.from_records(
            iter(cs), columns=[x[0] for x in cs.description])
        conn.close()
        return df["AUCTION_ROUND"].values[0]

    except Exception as e:
        logger.exception(e)
        return None


def get_end_of_month(month):
    month = month + timedelta(days=31)
    month = month.replace(day=1)
    month = month - timedelta(days=1)
    return month

# %%


def get_annual_ftr_auction(databasename, schemaname, tablename):
    site = "https://www.pjm.com/markets-and-operations/ftr.aspx"
    page = requests.get(site)
    tree = html.fromstring(page.content)
    files = []
    max_date = get_max_date("ANNUAL", databasename, schemaname, tablename) or datetime(2016, 12, 1)
    logger.info(max_date)
    try:
        for i in range(8, 15):
            path = f"/html/body/form/div[4]/div/div/div[2]/div/article/div/div[1]/div[{i}]/div/div/table//tr[position()]/td[1]/a/@href"
            table = tree.xpath(path)

            index = 0
            for file in table:
                if "results" in file:
                    datexpath = f"/html/body/form/div[4]/div/div/div[2]/div/article/div/div[1]/div[{i}]/div/div/table/tbody/tr[{1+index}]/td[2]"
                    if i > 12:
                        datexpath = f"/html/body/form/div[4]/div/div/div[2]/div/article/div/div[1]/div[{i}]/div/div/table/tbody/tr[{1+index}]/td[2]/div"

                    release_date = re.sub(
                        r"\D", "", tree.xpath(datexpath)[0].text)
                    release_date = datetime.strptime(release_date, "%m%d%Y")
                    if max_date is not None and release_date > max_date:
                        filename = [x.title()
                                    for x in file.split("/")[-1].split("-")[:-1]]
                        auction_name = " ".join(filename).split(".")[0]
                        auction_name = "PJMISO " + \
                            auction_name[:4] + "-" + auction_name[5:]
                        files.append((site, file, auction_name, release_date))
                    else:
                        break
                index = index + 1
    except Exception as e:
        print(e)

    return files


def get_longterm_ftr_auction(databasename, schemaname, tablename):
    site = "https://www.pjm.com/markets-and-operations/ftr.aspx"
    page = requests.get(site)
    tree = html.fromstring(page.content)
    path = "/html/body/form/div[4]/div/div/div[2]/div/article/div/div[1]/div[15]/div/div/table//tr[position()]/td[1]/a/@href"
    table = tree.xpath(path)

    files = []
    max_date = get_max_date("LONGTERM", databasename, schemaname, tablename) or datetime(2016, 12, 1)
    logger.info(max_date)
    try:
        for i in range(15, 20):
            index = 0
            path = f"/html/body/form/div[4]/div/div/div[2]/div/article/div/div[1]/div[{i}]/div/div/table//tr[position()]/td[1]/a/@href"
            table = tree.xpath(path)
            for file in table:
                if "results" in file:
                    datexpath = f"/html/body/form/div[4]/div/div/div[2]/div/article/div/div[1]/div[{i}]/div/div/table/tbody/tr[{1+index}]/td[2]"
                    if (i == 18 and index==0) or (i==17 and index==2):
                        datexpath = f"/html/body/form/div[4]/div/div/div[2]/div/article/div/div[1]/div[{i}]/div/div/table/tbody/tr[{1+index}]/td[2]/span"
                    elif i == 20:
                        datexpath = f"/html/body/form/div[4]/div/div/div[2]/div/article/div/div[1]/div[{i}]/div/div/table/tbody/tr[{1+index}]/td[2]/div"

                    release_date = re.sub(
                        r"\D", "", tree.xpath(datexpath)[0].text)
                    release_date = datetime.strptime(release_date, "%m%d%Y")
                    if max_date is not None and release_date > max_date:
                        filename = [x.title() for x in file.split(
                            "/")[-1].split(".")[0].split("-")]
                        filename.remove("Ftr")
                        filename.remove("Results")
                        auction_name = " ".join(filename)
                        auction_name = "PJMISO " + \
                            auction_name[:4] + "-" + \
                            auction_name[5:14] + "-" + auction_name[15:]
                        files.append((site, file, auction_name, release_date))
                    else:
                        break
                index = index + 1
    except Exception as e:
        print(e)

    return files


def get_monthly_ftr_auction(databasename, schemaname, tablename):
    site = "https://www.pjm.com/markets-and-operations/ftr/historical-ftr-auction.aspx"
    page = requests.get(site)
    soup = BeautifulSoup(page.content, "html.parser")
    iframexx = soup.find_all('iframe')
    for iframe in iframexx:
        if 'index.html' in iframe.attrs['src']:
            site = "https://www.pjm.com"+iframe.attrs['src'][8:]
            page = requests.get(site)

    tree = html.fromstring(page.content)
    path = "/html/body/table//tr[position()]/td[1]/a/@href"
    table = tree.xpath(path)

    files = []
    index = 0
    max_date = get_max_date("MONTHLY", databasename,
                            schemaname, tablename) or datetime(2016, 12, 1)
    logger.info(max_date)
    for file in table:
        datexpath = "/html/body/table//tr[{}]/td[2]".format(2+index)
        release_date = tree.xpath(datexpath)[0].text
        release_date = datetime.strptime(
            release_date[:-3], "%m/%d/%Y %H:%M:%S")
        if max_date is not None and release_date > max_date:
            auction_name = "PJMISO " + \
                file.split("-")[0][:4] + "-" + \
                file.split("-")[0][4:] + " Monthly Auction"
            files.append((site[:-10], file, auction_name, release_date))
        index = index + 1

    return files
# %%


def load_auction_constraint(auction_type, databasename, schemaname, tablename):

    if auction_type == "ANNUAL":
        files = get_annual_ftr_auction(databasename, schemaname, tablename)
    elif auction_type == "LONGTERM":
        files = get_longterm_ftr_auction(databasename, schemaname, tablename)
    elif auction_type == "MONTHLY":
        files = get_monthly_ftr_auction(databasename, schemaname, tablename)
    dfs = []

    table = databasename + '.' + schemaname+'.' + tablename
    query_primary_key = f'''SHOW PRIMARY KEYS IN {table}'''
    # conn = get_connection()
    cursor = get_connection().cursor()
    cursor.execute(query_primary_key)
    result = cursor.fetchall()
    if len(result) > 0:
        for j in range(0, len(result)):
            primary_key_list.append(result[j][4].upper())
    logger.info(f"Primary keys for table are {primary_key_list}")
    files.reverse()
    csv_file = ''
    for site, file, auction_name, release_date in files:
        try:
            r = None
            logger.info(site+file)
            try:
                for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(10)):
                    with attempt:
                        r = requests.get(site+file)
                        if r.status_code != 200:
                            raise Exception
            except RetryError:
                break
            csv_file = f"/Temp/pjm_constraints.xlsx"
            with open(csv_file, "wb") as csv:
                csv.write(r.content)
            csv.close()
            xlsxfile = pd.ExcelFile(csv_file)
            for sheet_name in xlsxfile.sheet_names:
                if "Binding Constraints" in sheet_name:
                    df = pd.read_excel(csv_file, sheet_name=sheet_name, skiprows=2, header=None,names=[
                                       'CONSTRAINT_NAME', 'CTG_ID', 'PERIOD_TYPE', 'ONPEAK_MARGINAL_VALUE', 'OFFPEAK_MARGINAL_VALUE'])
                    cols = ['CONSTRAINT_NAME', 'CTG_ID',
                            'PERIOD_TYPE', 'MARGINAL_VALUE']
                    offpeakdf = df[~df['OFFPEAK_MARGINAL_VALUE'].isnull(
                    )][['CONSTRAINT_NAME', 'CTG_ID', 'PERIOD_TYPE', 'OFFPEAK_MARGINAL_VALUE']]
                    offpeakdf.columns = cols
                    offpeakdf['BU_PEAKTYPE'] = 'OFFPEAK'
                    onpeakdf = df[~df['ONPEAK_MARGINAL_VALUE'].isnull(
                    )][['CONSTRAINT_NAME', 'CTG_ID', 'PERIOD_TYPE', 'ONPEAK_MARGINAL_VALUE']]
                    onpeakdf.columns = cols
                    onpeakdf['BU_PEAKTYPE'] = 'ONPEAK'
                    df = pd.concat([onpeakdf, offpeakdf])
                    df["BU_ISO"] = "PJMISO"
                    df["AUCTION_NAME"] = auction_name
                    df["AUCTION_TYPE"] = auction_type
                    year = auction_name.split(" ")[1]
                    if auction_type == "MONTHLY":
                        year = year.split("-")[0]
                    df["AUCTION_YEAR"] = year
                    df['AUCTION_MONTH'] = None
                    cols = list(df.columns)
                    cols = cols[-5:] + cols[:3] + cols[4:5] + cols[3:4]
                    df = df[cols]
                    df["AUCTION_ID"] = get_auction_id(auction_name)
                    df["AUCTION_ROUND"] = get_auction_round(auction_name)
                    df["RELEASE_DATE"] = release_date
                    df["INSERTED_DATE"] = datetime.now()
                    if auction_type == "ANNUAL":
                        df["FLOWSTART_DATE"] = datetime.strptime(
                            "{}0601".format(year.split("-")[0]), "%Y%m%d")
                        df["FLOWEND_DATE"] = datetime.strptime(
                            "{}0531".format(year.split("-")[1]), "%Y%m%d")
                    elif auction_type == "LONGTERM":
                        df["FLOWSTART_DATE"] = df["PERIOD_TYPE"].apply(lambda x: datetime.strptime(
                            "{}0601".format(str(int(x[-1]) - 1 + int(year.split("-")[0]))), "%Y%m%d"))
                        df["FLOWEND_DATE"] = df["FLOWSTART_DATE"].apply(
                            lambda x: datetime.strptime("{}0531".format(str(x.year + 1)), "%Y%m%d"))
                    elif auction_type == "MONTHLY":
                        result_month = datetime.strptime(
                            sheet_name.split(' ')[0], '%b').month
                        df['AUCTION_MONTH'] = result_month
                        df['START_END_DATE'] = df['PERIOD_TYPE'].apply(
                            lambda x: calc_start_end_date_by_period(x, year, result_month))
                        df[['FLOWSTART_DATE', 'FLOWEND_DATE']] = pd.DataFrame(
                            df.START_END_DATE.tolist(), index=df.index)
                        df = df.drop(columns='START_END_DATE')

                    df.rename(columns={'FLOWSTART_DATE': 'FLOW_STARTDATE',
                                       'FLOWEND_DATE': 'FLOW_ENDDATE', 'CONSTRAINT_NAME': 'CONSTRAINT'}, inplace=True)
                    df.drop_duplicates(subset=primary_key_list,
                                       keep='first', inplace=True)
                    dfs.append(df)
        except Exception as e:
            logger.exception(f'Error {e}')
            break
    if len(dfs) > 0:
        try:
            os.remove(csv_file)
        except PermissionError:
            logger.info('no permission to del file.')
        finaldf = pd.concat(dfs)
        return finaldf
    else:
        return None


def calc_start_end_date_by_period(period: str, year: str, result_month: int):
    quater_start_mappings = {'Q1': 'JUN',
                             'Q2': 'SEP', 'Q3': 'DEC', 'Q4': 'MAR'}
    quater_end_mappings = {'Q1': 'AUG', 'Q2': 'NOV', 'Q3': 'FEB', 'Q4': 'MAY'}
    start_month, end_month = '', ''
    if 'Q' in period:
        start_month = quater_start_mappings[period]
        end_month = quater_end_mappings[period]
    else:
        period_split = period.split('-')
        start_month = period_split[0]
        end_month = period_split[-1]

    end_month_num = datetime.strptime(end_month, "%b").month
    # logger.info(start_month_num, end_month_num, result_month, year)
    if end_month_num < result_month:
        start_date = datetime.strptime(
            "{}{}01".format(int(year)+1, start_month), "%Y%b%d")
    else:
        start_date = datetime.strptime(
            "{}{}01".format(int(year), start_month), "%Y%b%d")
    end_date = get_end_of_month(start_date.replace(month=end_month_num))
    return [start_date, end_date]


if __name__ == "__main__":
    import bu_alerts
    sys.path.append(pathlib.Path().absolute().__str__())
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logfilename = bu_alerts.add_file_logging(logger,process_name='pjm_auction_constraint')
    logger.info(f'Execution started')
    sys.path.append(
        r'\\biourja.local\biourja\Groups\Gas & Pwr\Pwr\FTR\Virtuals\Michael\bulog')
    sys.path.append(
        r'\\biourja.local\biourja\Groups\Gas & Pwr\Pwr\FTR\Virtuals\Michael\FTR\sftp_to_sf')
    from bulog import get_connection
    from tosnowflake import load_df_to_sf, sendmail

    try:
        bu_alerts.bulog(
            process_name='PJM_AUCTION_CONSTRAINTS',
            status='started',
            table_name='PJM_AUCTION_CONSTRAINTS',
            process_owner='Radha/Rahul',
            database=databasename

        )
        auction_types = ["MONTHLY" ,"LONGTERM","ANNUAL"]
        results = []
        for auction_type in auction_types:
            df = load_auction_constraint(
                auction_type, databasename, schemaname, tablename)
            # df = pd.read_csv(r'/temp/to_snowflake75574.csv')
            if df is not None:
                res = load_df_to_sf(
                    df, databasename, schemaname, tablename, primary_key_list, uploadinsert=False)
                if res is not None:
                    resitem = (auction_type, res, str(datetime.now()))
                    results.append(resitem)
        # %%
        logger.info(results)
        # to_addr = ["ftr@biourja.com","indiapowerit@biourja.com"]
        bu_alerts.bulog(
            process_name='PJM_AUCTION_CONSTRAINTS',
            status='started',
            table_name='PJM_AUCTION_CONSTRAINTS',
            database=databasename,
            row_count=len(results),
            process_owner='Radha/Rahul'
        )
        bu_alerts.send_mail(
            sender_email="biourjapowerdata@biourja.com",
            sender_password=r"bY3mLSQ-\Q!9QmXJ",
            receiver_email="indiapowerit@biourja.com, DAPower@biourja.com",
            mail_subject="JOB SUCCESS - PJM_AUCTION_CONSTRAINTS",
            mail_body=f"PJM_AUCTION_CONSTRAINTS completed successfully, Attached logs",
            attachment_location=logfilename)
        #     mailsubj = 'PJMISO AUCTION CONSTRAINT -> Snowflake'
        #     df = pd.DataFrame(results, columns=["Auction Type", "Number of Rows", "Insert Date"])

        #     msgbody = '<table style="border-spacing: 5px">'
        #     msgbody = msgbody + '<tr>'

        #     for x in list(df.columns):
        #         msgbody = msgbody + '<td style="font-weight: bold; text-align:left; background-color: light grey">{}</td>'.format(x)
        #     msgbody = msgbody + '</tr>'

        #     for i, x in df.iterrows():
        #         msgbody = msgbody + '<tr>'
        #         for y in list(df.columns):
        #             msgbody = msgbody + '<td style="text-align:left">{}</td>'.format(x[y])
        #         msgbody = msgbody + '</tr>'
        #     msgbody = msgbody + '</table>'
        # sendmail(to_addr, mailsubj, msgbody)
        # %%
        # )
    except Exception as e:
        logger.exception(f'Error occured due to {e}')
        bu_alerts.bulog(
            process_name='PJM_AUCTION_CONSTRAINTS',
            status='failed',
            table_name='PJM_AUCTION_CONSTRAINTS',
            database=databasename,
            process_owner='Radha/Rahul'
        )
        bu_alerts.send_mail(
            receiver_email="indiapowerit@biourja.com, DAPower@biourja.com",
            mail_subject="JOB FAILED - PJM_AUCTION_CONSTRAINTS",
            mail_body=f"PJM_AUCTION_CONSTRAINTS failed with error {e}, Attached logs",
            attachment_location=logfilename
        )
