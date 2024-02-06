#%%
import logging
import sys
import os
# sys.path.append(r'\\biourja.local\biourja\Groups\Gas & Pwr\Pwr\FTR\Virtuals\Michael\FTR\sftp_to_sf')
# sys.path.append(r'\\biourja.local\biourja\Data\IT Dev\VCRON10_Production_Environment\pjm_iso_website_data')
from tosnowflake import load_df_to_sf,get_max_datetime
import requests
import json
import yaml
import sharepy
import re
import os
import math
import pandas as pd
import numpy as np
from datetime import datetime
from bu_snowflake import get_connection
import bu_alerts
import shutil
# To get credentials used in process	
from bu_config import get_config

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# log file location
log_file_location = os.getcwd() + '\\' + 'logs' + '\\' + 'PJM_LINE_RATINGS_LOG.txt'
if os.path.isfile(log_file_location):
    os.remove(log_file_location)

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] - %(message)s',
    filename=log_file_location)

job_id=np.random.randint(1000000,9999999)

def store_file_to_onedrive(content):
    here = os.path.dirname(os.path.abspath(__file__))
    f = yaml.safe_load(open(here+r'/credentials.yaml', 'r'))
    username = f['username']
    password = f['password']
    site = 'https://biourja.sharepoint.com'
    path1 = "/BiourjaPower/_api/web/GetFolderByServerRelativeUrl"
    # Connecting to Sharepoint and downloading the file with sync params
    s = sharepy.connect(site, username, password)      
    today_date = datetime.now().strftime("%Y_%m_%d")
    spfile2 = f'PJM_line_ratings_{today_date}.txt'
    # path2 = "('Shared Documents/ARCHIVE')/Files{}"
    path2 = "('Shared Documents/ISO''s & Power Markets//PJM/PJM_line_rating_backup')/Files{}"
    headers = {"accept": "application/json;odata=verbose",
        "content-type": "application/x-www-urlencoded; charset=UTF-8"}
    if "IP Address has not been updated" in content:
        print("False")
        return False
    else:
        p = s.post(site + path1 + path2.format("/add(url='"+spfile2+"',overwrite=true)"), data=content, headers=headers)
        # logging.info(f" Status Code :: {p.status_code}")
        print(f" Status Code :: {p.status_code}")
        return True

def load_pjm_line_ratings(url, username, password, filename, databasename, schemaname, tablename):
    df_list = []
    try:
        res = None
        extract_dir_name = 'pjm_line_ratings'
        downlaod_zip_file_name = 'pjm_line_ratings.zip'
        root_dir = os.getcwd()
        file_path = root_dir + '\\' + extract_dir_name + '\\'
        os.chdir(root_dir)
        print(root_dir)
        print(file_path)
        shutil.rmtree(file_path)
        print("downlaod file at location")
        download_file_response = requests.get(url,stream=True)
        print("download zip file")
        with open(downlaod_zip_file_name, 'wb') as file:
            file.write(download_file_response.content)
        shutil.unpack_archive(downlaod_zip_file_name, root_dir + '\\' + extract_dir_name, 'zip')
        for files in os.walk(extract_dir_name):
            print(files)
            for file in files:
                if '.txt' in str(file):
                    file = str(file).replace("['","")
                    file = str(file).replace("']","")
                    abs_path = file_path + file
                    with open(abs_path) as f:
                        contents = f.read()
                    # print(contents)
        # r = requests.get(url)
        # content_utf8 = contents.decode('UTF-8') 
        if '(throttle 1800 seconds)' not in contents:
            content_utf8 = contents
            flag = store_file_to_onedrive(content_utf8)
            logging.info(f"File backup Status:::::: {flag}")
            content = content_utf8.split("Substn")
            for i in range(1, len(content)):
                content[i] = "Substn" + content[i]

            posted_date = content[0].split("Posted at ")[1][:19]
            #posted_date = posted_date[6:] + "-" + posted_date[:5] 
            content = content[1:]
            max_date = get_max_datetime(databasename, schemaname, tablename, "POSTED_DATE")
            try:
                
                if max_date >= datetime.strptime(posted_date, "%m-%d-%Y %H:%M:%S"):
                    posted_date_temp = datetime.strptime(posted_date,"%m-%d-%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S").__str__()
                    conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='ISO')
                    conn.cursor().execute("use warehouse quant_wh")
                    #conn.cursor().execute("USE ROLE OWNER_POWERDB_BIZDEV")
                    conn.cursor().execute("use database {}".format(databasename))
                    conn.cursor().execute("delete from {} where posted_date = '{}'".format(schemaname+"."+tablename, posted_date_temp))
                    print("Rows for {} removed.".format(posted_date))
            except Exception as e:
                logging.exception(f'Exception caught::::{e}')

            substn_a = [x for x in content if "End: END A" in x or "End: LOW" in x]
            substn_b = [x for x in content if "End: END B" in x or "End: HIGH" in x]

            for substn in substn_a:
                substn = substn.split("\n")
                line_name = substn[0]
                start = re.search(": ", line_name)
                end = re.search("kV", line_name)
                substation_end_1 = line_name[start.end():end.start()]
                line_name = line_name[end.start():]
                substation_end_1 = re.sub(r"\s+$", "", substation_end_1)
                start = re.search(": ", line_name)
                end = re.search("Dev", line_name)
                kv_end_1 = line_name[start.end():end.start()]
                line_name = line_name[end.start():]
                kv_end_1 = re.sub(r"\s+$", "", kv_end_1)
                start = re.search(": ", line_name)
                end = re.search("End", line_name)
                dev_end_1 = line_name[start.end():end.start()]
                dev_end_1 = re.sub(r"\s+$", "", dev_end_1)
                line_name = substation_end_1 + "_" + kv_end_1 + "_" + dev_end_1
                line_name_1 = re.sub("\s+$", "",substn[1].split(": ")[1])
                end_1_day_41 = [x for x in substn[10].split(" ") if len(x) > 0]
                end_1_day_41_norm_cap = int(end_1_day_41[1])
                end_1_day_41_long_cap = int(end_1_day_41[2])
                end_1_day_41_short_cap = int(end_1_day_41[3])
                end_1_day_95 = [x for x in substn[4].split(" ") if len(x) > 0]
                end_1_day_95_norm_cap = int(end_1_day_95[1])
                end_1_day_95_long_cap = int(end_1_day_95[2])
                end_1_day_95_short_cap = int(end_1_day_95[3])

                match = None
                for search in substn_b:
                    if substn[1] in search:
                        match = search.split("\n")
                        substn_b.remove(search)
                        break

                if match is None:
                    line_name_2 = None
                    dev_end_2 = None
                    kv_end_2 = None
                    substation_end_2 = None
                    end_2_day_41_norm_cap = None
                    end_2_day_41_long_cap = None
                    end_2_day_41_short_cap = None
                    end_2_day_95_norm_cap = None
                    end_2_day_95_long_cap = None
                    end_2_day_95_short_cap = None

                    all_day = []
                    all_night = []
                    norm_day = []
                    norm_night = []

                    for i in range(4, 12):
                        vals = [int(re.sub("\r", "", x)) for x in substn[i].split(" ") if len(x) > 0]
                        all_day.extend(vals[1:5])
                        all_night.extend(vals[5:])
                        norm_day.append(vals[1])
                        norm_night.append(vals[5])

                    all_day.sort()
                    all_night.sort()
                    max_day_cap = all_day[-1]
                    min_day_cap = all_day[0]
                    mid = len(all_day) // 2
                    median_day_cap = math.ceil((all_day[mid] + all_day[~mid]) / 2)
                    max_night_cap = all_night[-1]
                    min_night_cap = all_night[0]
                    mid = len(all_night) // 2
                    median_night_cap = math.ceil((all_night[mid] + all_night[~mid]) / 2)
                    max_norm_day_cap = max(norm_day)
                    min_norm_day_cap = min(norm_day)
                    max_norm_night_cap = max(norm_night)
                    min_norm_night_cap = min(norm_night)
                    capacity_mw_summer = end_1_day_95_norm_cap
                    capacity_mw_winter = end_1_day_41_norm_cap
                
                else:
                    line_name_2 = re.sub("\s+$", "", match[1].split(": ")[1])
                    dev_end_2 = dev_end_1
                    kv_end_2 = kv_end_1
                    substation_end_2 = substation_end_1
                    end_2_day_41 = [x for x in match[10].split(" ") if len(x) > 0]
                    end_2_day_41_norm_cap = int(end_2_day_41[1])
                    end_2_day_41_long_cap = int(end_2_day_41[2])
                    end_2_day_41_short_cap = int(end_2_day_41[3])
                    end_2_day_95 = [x for x in match[4].split(" ") if len(x) > 0]
                    end_2_day_95_norm_cap = int(end_2_day_95[1])
                    end_2_day_95_long_cap = int(end_2_day_95[2])
                    end_2_day_95_short_cap = int(end_2_day_95[3])

                    all_day = []
                    all_night = []
                    norm_day = []
                    norm_night = []

                    for i in range(4, 12):
                        vals = [int(re.sub("\r", "", x)) for x in substn[i].split(" ") if len(x) > 0]
                        all_day.extend(vals[1:5])
                        all_night.extend(vals[5:])
                        norm_day.append(vals[1])
                        norm_night.append(vals[5])

                        bvals = [int(re.sub("\r", "", x)) for x in match[i].split(" ") if len(x) > 0]
                        all_day.extend(bvals[1:5])
                        all_night.extend(bvals[5:])
                        norm_day.append(bvals[1])
                        norm_night.append(bvals[5])

                    all_day.sort()
                    all_night.sort()
                    max_day_cap = all_day[-1]
                    min_day_cap = all_day[0]
                    mid = len(all_day) // 2
                    median_day_cap = math.ceil((all_day[mid] + all_day[~mid]) / 2)
                    max_night_cap = all_night[-1]
                    min_night_cap = all_night[0]
                    mid = len(all_night) // 2
                    median_night_cap = math.ceil((all_night[mid] + all_night[~mid]) / 2)
                    max_norm_day_cap = max(norm_day)
                    min_norm_day_cap = min(norm_day)
                    max_norm_night_cap = max(norm_night)
                    min_norm_night_cap = min(norm_night)
                    capacity_mw_summer = end_1_day_95_norm_cap
                    capacity_mw_winter = end_1_day_41_norm_cap

                df_item = (line_name, line_name_1, line_name_2, dev_end_1, dev_end_2, kv_end_1, kv_end_2,
                        substation_end_1, substation_end_2, end_1_day_41_norm_cap, end_1_day_41_long_cap,
                        end_1_day_41_short_cap, end_1_day_95_norm_cap, end_1_day_95_long_cap, 
                        end_1_day_95_short_cap, end_2_day_41_norm_cap, end_2_day_41_long_cap,
                        end_2_day_41_short_cap, end_2_day_95_norm_cap, end_2_day_95_long_cap,
                        end_2_day_95_short_cap, max_day_cap, min_day_cap, median_day_cap, max_night_cap,
                        min_night_cap, median_night_cap, max_norm_day_cap, min_norm_day_cap,
                        max_norm_night_cap, min_norm_night_cap, capacity_mw_summer, capacity_mw_winter)

                df_list.append(df_item)

            for substn in substn_b:
                substn = substn.split("\n")
                line_name = substn[0]
                start = re.search(": ", line_name)
                end = re.search("kV", line_name)
                substation_end_2 = line_name[start.end():end.start()]
                line_name = line_name[end.start():]
                substation_end_2 = re.sub(r"\s+$", "", substation_end_2)
                start = re.search(": ", line_name)
                end = re.search("Dev", line_name)
                kv_end_2 = line_name[start.end():end.start()]
                line_name = line_name[end.start():]
                kv_end_2 = re.sub(r"\s+$", "", kv_end_2)
                start = re.search(": ", line_name)
                end = re.search("End", line_name)
                dev_end_2 = line_name[start.end():end.start()]
                dev_end_2 = re.sub(r"\s+$", "", dev_end_2)
                line_name = substation_end_2 + "_" + kv_end_2 + "_" + dev_end_2
                line_name_2 = re.sub("\s+$", "",substn[1].split(": ")[1])
                end_2_day_41 = [x for x in substn[10].split(" ") if len(x) > 0]
                end_2_day_41_norm_cap = int(end_2_day_41[1])
                end_2_day_41_long_cap = int(end_2_day_41[2])
                end_2_day_41_short_cap = int(end_2_day_41[3])
                end_2_day_95 = [x for x in substn[4].split(" ") if len(x) > 0]
                end_2_day_95_norm_cap = int(end_2_day_95[1])
                end_2_day_95_long_cap = int(end_2_day_95[2])
                end_2_day_95_short_cap = int(end_2_day_95[3])

                line_name_1 = None
                dev_end_1 = None
                kv_end_1 = None
                substation_end_1 = None
                end_1_day_41_norm_cap = None
                end_1_day_41_long_cap = None
                end_1_day_41_short_cap = None
                end_1_day_95_norm_cap = None
                end_1_day_95_long_cap = None
                end_1_day_95_short_cap = None

                all_day = []
                all_night = []
                norm_day = []
                norm_night = []

                for i in range(4, 12):
                    vals = [int(re.sub("\r", "", x)) for x in substn[i].split(" ") if len(x) > 0]
                    all_day.extend(vals[1:5])
                    all_night.extend(vals[5:])
                    norm_day.append(vals[1])
                    norm_night.append(vals[5])

                all_day.sort()
                all_night.sort()
                max_day_cap = all_day[-1]
                min_day_cap = all_day[0]
                mid = len(all_day) // 2
                median_day_cap = math.ceil((all_day[mid] + all_day[~mid]) / 2)
                max_night_cap = all_night[-1]
                min_night_cap = all_night[0]
                mid = len(all_night) // 2
                median_night_cap = math.ceil((all_night[mid] + all_night[~mid]) / 2)
                max_norm_day_cap = max(norm_day)
                min_norm_day_cap = min(norm_day)
                max_norm_night_cap = max(norm_night)
                min_norm_night_cap = min(norm_night)
                capacity_mw_summer = None
                capacity_mw_winter = None

                df_item = (line_name, line_name_1, line_name_2, dev_end_1, dev_end_2, kv_end_1, kv_end_2,
                        substation_end_1, substation_end_2, end_1_day_41_norm_cap, end_1_day_41_long_cap,
                        end_1_day_41_short_cap, end_1_day_95_norm_cap, end_1_day_95_long_cap, 
                        end_1_day_95_short_cap, end_2_day_41_norm_cap, end_2_day_41_long_cap,
                        end_2_day_41_short_cap, end_2_day_95_norm_cap, end_2_day_95_long_cap,
                        end_2_day_95_short_cap, max_day_cap, min_day_cap, median_day_cap, max_night_cap,
                        min_night_cap, median_night_cap, max_norm_day_cap, min_norm_day_cap,
                        max_norm_night_cap, min_norm_night_cap, capacity_mw_summer, capacity_mw_winter)

                df_list.append(df_item)

            df = pd.DataFrame(df_list)
            cols = ["line_name", "line_name_1", "line_name_2", "dev_end_1", "dev_end_2", "kv_end_1", "kv_end_2",
                    "substation_end_1", "substation_end_2", "end_1_day_41_norm_cap", "end_1_day_41_long_cap",
                    "end_1_day_41_short_cap", "end_1_day_95_norm_cap", "end_1_day_95_long_cap", 
                    "end_1_day_95_short_cap", "end_2_day_41_norm_cap", "end_2_day_41_long_cap",
                    "end_2_day_41_short_cap", "end_2_day_95_norm_cap", "end_2_day_95_long_cap",
                    "end_2_day_95_short_cap", "max_day_cap", "min_day_cap", "median_day_cap", "max_night_cap",
                    "min_night_cap", "median_night_cap", "max_norm_day_cap", "min_norm_day_cap",
                    "max_norm_night_cap", "min_norm_night_cap", "capacity_mw_summer", "capacity_mw_winter"]
            df.columns = [x.upper() for x in cols]
            df["POSTED_DATE"] = datetime.strptime(posted_date, "%m-%d-%Y %H:%M:%S")
            pks = ["LINE_NAME", "POSTED_DATE"]
            print("Length of dataframe before drop_duplicates for pjm_line_ratings::::::",len(df))
            df.drop_duplicates(subset=pks,keep='first',inplace=True)
            print("Length of dataframe after drop_duplicates for pjm_line_ratings:::::::",len(df))
            df["INSERT_DATE"] = datetime.now()
            df["UPDATE_DATE"] = datetime.now()
            df = df[['LINE_NAME', 'LINE_NAME_1', 'LINE_NAME_2', 'DEV_END_1', 'DEV_END_2', 'KV_END_1', 'KV_END_2', 'SUBSTATION_END_1', 'SUBSTATION_END_2', 'END_1_DAY_41_NORM_CAP', 'END_1_DAY_41_LONG_CAP', 'END_1_DAY_41_SHORT_CAP', 'END_1_DAY_95_NORM_CAP', 'END_1_DAY_95_LONG_CAP', 'END_1_DAY_95_SHORT_CAP', 'END_2_DAY_41_NORM_CAP', 'END_2_DAY_41_LONG_CAP', 'END_2_DAY_41_SHORT_CAP', 'END_2_DAY_95_NORM_CAP', 'END_2_DAY_95_LONG_CAP', 'END_2_DAY_95_SHORT_CAP', 'MAX_DAY_CAP', 'MIN_DAY_CAP', 'MEDIAN_DAY_CAP', 'MAX_NIGHT_CAP', 'MIN_NIGHT_CAP', 'MEDIAN_NIGHT_CAP', 'MAX_NORM_DAY_CAP', 'MIN_NORM_DAY_CAP', 'MAX_NORM_NIGHT_CAP', 'MIN_NORM_NIGHT_CAP', 'CAPACITY_MW_SUMMER', 'CAPACITY_MW_WINTER', 'UPDATE_DATE', 'POSTED_DATE', 'INSERT_DATE']]

            res = load_df_to_sf(df, databasename, schemaname, tablename, pks, uploadinsert=False)
        if res is not None:
            return res
        else:
            return 0
    except Exception as e:
        print("Exception caught during execution: ",e)
        logging.exception(f'Exception caught during execution: {e}')
        raise e

def load_pjm_temp_line_ratings(url, username, password, filename, databasename, schemaname, tablename):
    try:
        res = None
        extract_dir_name = 'pjm_temp_line_ratings'
        downlaod_zip_file_name = 'pjm_temp_line_ratings.zip'
        root_dir = os.getcwd()
        file_path = root_dir + '\\' + extract_dir_name + '\\'
        os.chdir(root_dir)
        print(root_dir)
        print(file_path)
        # shutil.rmtree(file_path)
        print("downlaod file at location")
        download_file_response = requests.get(url,stream=True)
        print("download zip file")
        with open(downlaod_zip_file_name, 'wb') as file:
            file.write(download_file_response.content)
        shutil.unpack_archive(downlaod_zip_file_name, root_dir + '\\' + extract_dir_name, 'zip')
        for files in os.walk(extract_dir_name):
            print(files)
            for file in files:
                if '.txt' in str(file):
                    file = str(file).replace("['","")
                    file = str(file).replace("']","")
                    abs_path = file_path + file
                    with open(abs_path) as f:
                        contents = f.read()
                    # print(contents)
        # r = requests.get(url)
        # content_utf8 = contents.decode('UTF-8') 
        if '(throttle 1800 seconds)' not in contents:
        # r = requests.get(url)
            content = contents.split("Substn")
            for i in range(1, len(content)):
                content[i] = "Substn" + content[i]

            posted_date = content[0].split("Posted at ")[1][:19]
            #posted_date = posted_date[6:] + "-" + posted_date[:5] 
            content = content[1:]
            df_list = []

            max_date = get_max_datetime(databasename, schemaname, tablename, "POSTED_DATE")

            if max_date >= datetime.strptime(posted_date, "%m-%d-%Y %H:%M:%S"):
                posted_date_temp = datetime.strptime(posted_date,"%m-%d-%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S").__str__()
                conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='ISO')
                conn.cursor().execute("use warehouse quant_wh")
                #conn.cursor().execute("USE ROLE OWNER_POWERDB_BIZDEV")
                conn.cursor().execute("use database {}".format(databasename))
                conn.cursor().execute("delete from {} where posted_date = '{}'".format(schemaname+"."+tablename, posted_date_temp))
                print("Rows for {} removed.".format(posted_date))


            substn_a = [x for x in content if "End: END A" in x or "End: LOW" in x]
            substn_b = [x for x in content if "End: END B" in x or "End: HIGH" in x]

            for substn in substn_a:
                substn = substn.split("\n")
                line_name = substn[0]
                start = re.search(": ", line_name)
                end = re.search("kV", line_name)
                substation_end_1 = line_name[start.end():end.start()]
                line_name = line_name[end.start():]
                substation_end_1 = re.sub(r"\s+$", "", substation_end_1)
                start = re.search(": ", line_name)
                end = re.search("Dev", line_name)
                kv_end_1 = line_name[start.end():end.start()]
                line_name = line_name[end.start():]
                kv_end_1 = re.sub(r"\s+$", "", kv_end_1)
                start = re.search(": ", line_name)
                end = re.search("End", line_name)
                dev_end_1 = line_name[start.end():end.start()]
                dev_end_1 = re.sub(r"\s+$", "", dev_end_1)
                line_name = substation_end_1 + "_" + kv_end_1 + "_" + dev_end_1
                line_name_1 = re.sub("\s+$", "",substn[1].split(": ")[1])
                reason_end_1 = substn[2].split(": ")[1]
                info = substn[3].split(": ")
                start_date_end_1 = datetime.strptime(info[1][:-8], "%m/%d/%Y %H:%M")
                end_date_end_1 = datetime.strptime(info[2][:-7], "%m/%d/%Y %H:%M")
                start_date = start_date_end_1
                end_date = end_date_end_1
                status_end_1 = info[-1][:-1]
                end_1 = "END A"
                end_1_day_41 = [x for x in substn[12].split(" ") if len(x) > 0]
                end_1_day_41_norm_cap = int(end_1_day_41[1])
                end_1_day_41_long_cap = int(end_1_day_41[2])
                end_1_day_41_short_cap = int(end_1_day_41[3])
                end_1_day_95 = [x for x in substn[6].split(" ") if len(x) > 0]
                end_1_day_95_norm_cap = int(end_1_day_95[1])
                end_1_day_95_long_cap = int(end_1_day_95[2])
                end_1_day_95_short_cap = int(end_1_day_95[3])

                match = None
                for search in substn_b:
                    if substn[1] in search:
                        match = search.split("\n")
                        substn_b.remove(search)
                        break

                if match is None:
                    line_name_2 = None
                    reason_end_2 = None
                    start_date_end_2 = None
                    end_date_end_2 = None
                    status_end_2 = None
                    end_2 = None
                    dev_end_2 = None
                    kv_end_2 = None
                    substation_end_2 = None
                    end_2_day_41_norm_cap = None
                    end_2_day_41_long_cap = None
                    end_2_day_41_short_cap = None
                    end_2_day_95_norm_cap = None
                    end_2_day_95_long_cap = None
                    end_2_day_95_short_cap = None

                    all_day = []
                    all_night = []
                    norm_day = []
                    norm_night = []

                    for i in range(6, 14):
                        vals = [int(re.sub("\r", "", x)) for x in substn[i].split(" ") if len(x) > 0]
                        all_day.extend(vals[1:5])
                        all_night.extend(vals[5:])
                        norm_day.append(vals[1])
                        norm_night.append(vals[5])

                    all_day.sort()
                    all_night.sort()
                    max_day_cap = all_day[-1]
                    min_day_cap = all_day[0]
                    mid = len(all_day) // 2
                    median_day_cap = math.ceil((all_day[mid] + all_day[~mid]) / 2)
                    max_night_cap = all_night[-1]
                    min_night_cap = all_night[0]
                    mid = len(all_night) // 2
                    median_night_cap = math.ceil((all_night[mid] + all_night[~mid]) / 2)
                    max_norm_day_cap = max(norm_day)
                    min_norm_day_cap = min(norm_day)
                    max_norm_night_cap = max(norm_night)
                    min_norm_night_cap = min(norm_night)
                    capacity_mw_summer = end_1_day_95_norm_cap
                    capacity_mw_winter = end_1_day_41_norm_cap
                
                else:
                    line_name_2 = re.sub("\s+$", "", match[1].split(": ")[1])
                    reason_end_2 = match[2].split(": ")[1]
                    info = match[3].split(": ")
                    start_date_end_2 = datetime.strptime(info[1][:-8], "%m/%d/%Y %H:%M")
                    end_date_end_2 = datetime.strptime(info[2][:-7], "%m/%d/%Y %H:%M")
                    status_end_2 = info[-1][:-1]
                    end_2 = "END B"
                    dev_end_2 = dev_end_1
                    kv_end_2 = kv_end_1
                    substation_end_2 = substation_end_1
                    end_2_day_41 = [x for x in match[12].split(" ") if len(x) > 0]
                    end_2_day_41_norm_cap = int(end_2_day_41[1])
                    end_2_day_41_long_cap = int(end_2_day_41[2])
                    end_2_day_41_short_cap = int(end_2_day_41[3])
                    end_2_day_95 = [x for x in match[6].split(" ") if len(x) > 0]
                    end_2_day_95_norm_cap = int(end_2_day_95[1])
                    end_2_day_95_long_cap = int(end_2_day_95[2])
                    end_2_day_95_short_cap = int(end_2_day_95[3])

                    all_day = []
                    all_night = []
                    norm_day = []
                    norm_night = []

                    for i in range(6, 14):
                        vals = [int(re.sub("\r", "", x)) for x in substn[i].split(" ") if len(x) > 0]
                        all_day.extend(vals[1:5])
                        all_night.extend(vals[5:])
                        norm_day.append(vals[1])
                        norm_night.append(vals[5])

                        bvals = [int(re.sub("\r", "", x)) for x in match[i].split(" ") if len(x) > 0]
                        all_day.extend(bvals[1:5])
                        all_night.extend(bvals[5:])
                        norm_day.append(bvals[1])
                        norm_night.append(bvals[5])

                    all_day.sort()
                    all_night.sort()
                    max_day_cap = all_day[-1]
                    min_day_cap = all_day[0]
                    mid = len(all_day) // 2
                    median_day_cap = math.ceil((all_day[mid] + all_day[~mid]) / 2)
                    max_night_cap = all_night[-1]
                    min_night_cap = all_night[0]
                    mid = len(all_night) // 2
                    median_night_cap = math.ceil((all_night[mid] + all_night[~mid]) / 2)
                    max_norm_day_cap = max(norm_day)
                    min_norm_day_cap = min(norm_day)
                    max_norm_night_cap = max(norm_night)
                    min_norm_night_cap = min(norm_night)
                    capacity_mw_summer = end_1_day_95_norm_cap
                    capacity_mw_winter = end_1_day_41_norm_cap

                df_item = (line_name, line_name_1, line_name_2, reason_end_1, reason_end_2, 
                            start_date_end_1, end_date_end_1, start_date_end_2, end_date_end_2, start_date, end_date,
                            status_end_1, status_end_2, end_1, end_2, dev_end_1, dev_end_2, kv_end_1, kv_end_2,
                            substation_end_1, substation_end_2, end_1_day_41_norm_cap, end_1_day_41_long_cap,
                            end_1_day_41_short_cap, end_1_day_95_norm_cap, end_1_day_95_long_cap, 
                            end_1_day_95_short_cap, end_2_day_41_norm_cap, end_2_day_41_long_cap,
                            end_2_day_41_short_cap, end_2_day_95_norm_cap, end_2_day_95_long_cap,
                            end_2_day_95_short_cap, max_day_cap, min_day_cap, median_day_cap, max_night_cap,
                            min_night_cap, median_night_cap, max_norm_day_cap, min_norm_day_cap,
                            max_norm_night_cap, min_norm_night_cap, capacity_mw_summer, capacity_mw_winter)

                df_list.append(df_item)

            for substn in substn_b:
                substn = substn.split("\n")
                line_name = substn[0]
                start = re.search(": ", line_name)
                end = re.search("kV", line_name)
                substation_end_2 = line_name[start.end():end.start()]
                line_name = line_name[end.start():]
                substation_end_2 = re.sub(r"\s+$", "", substation_end_2)
                start = re.search(": ", line_name)
                end = re.search("Dev", line_name)
                kv_end_2 = line_name[start.end():end.start()]
                line_name = line_name[end.start():]
                kv_end_2 = re.sub(r"\s+$", "", kv_end_2)
                start = re.search(": ", line_name)
                end = re.search("End", line_name)
                dev_end_2 = line_name[start.end():end.start()]
                dev_end_2 = re.sub(r"\s+$", "", dev_end_2)
                line_name = substation_end_2 + "_" + kv_end_2 + "_" + dev_end_2
                line_name_2 = re.sub("\s+$", "",substn[1].split(": ")[1])
                reason_end_2 = substn[2].split(": ")[1]
                info = substn[3].split(": ")
                start_date_end_2 = datetime.strptime(info[1][:-8], "%m/%d/%Y %H:%M")
                end_date_end_2 = datetime.strptime(info[2][:-7], "%m/%d/%Y %H:%M")
                start_date = start_date_end_2
                end_date = end_date_end_2
                status_end_2 = info[-1][:-1]
                end_2 = "END B"
                end_2_day_41 = [x for x in substn[12].split(" ") if len(x) > 0]
                end_2_day_41_norm_cap = int(end_2_day_41[1])
                end_2_day_41_long_cap = int(end_2_day_41[2])
                end_2_day_41_short_cap = int(end_2_day_41[3])
                end_2_day_95 = [x for x in substn[6].split(" ") if len(x) > 0]
                end_2_day_95_norm_cap = int(end_2_day_95[1])
                end_2_day_95_long_cap = int(end_2_day_95[2])
                end_2_day_95_short_cap = int(end_2_day_95[3])

                line_name_1 = None
                reason_end_1 = None
                start_date_end_1 = None
                end_date_end_1 = None
                status_end_1 = None
                end_1 = None
                dev_end_1 = None
                kv_end_1 = None
                substation_end_1 = None
                end_1_day_41_norm_cap = None
                end_1_day_41_long_cap = None
                end_1_day_41_short_cap = None
                end_1_day_95_norm_cap = None
                end_1_day_95_long_cap = None
                end_1_day_95_short_cap = None

                all_day = []
                all_night = []
                norm_day = []
                norm_night = []

                for i in range(6, 14):
                    vals = [int(re.sub("\r", "", x)) for x in substn[i].split(" ") if len(x) > 0]
                    all_day.extend(vals[1:5])
                    all_night.extend(vals[5:])
                    norm_day.append(vals[1])
                    norm_night.append(vals[5])

                all_day.sort()
                all_night.sort()
                max_day_cap = all_day[-1]
                min_day_cap = all_day[0]
                mid = len(all_day) // 2
                median_day_cap = math.ceil((all_day[mid] + all_day[~mid]) / 2)
                max_night_cap = all_night[-1]
                min_night_cap = all_night[0]
                mid = len(all_night) // 2
                median_night_cap = math.ceil((all_night[mid] + all_night[~mid]) / 2)
                max_norm_day_cap = max(norm_day)
                min_norm_day_cap = min(norm_day)
                max_norm_night_cap = max(norm_night)
                min_norm_night_cap = min(norm_night)
                capacity_mw_summer = None
                capacity_mw_winter = None

                df_item = (line_name, line_name_1, line_name_2, reason_end_1, reason_end_2, 
                            start_date_end_1, end_date_end_1, start_date_end_2, end_date_end_2, start_date, end_date,
                            status_end_1, status_end_2, end_1, end_2, dev_end_1, dev_end_2, kv_end_1, kv_end_2,
                            substation_end_1, substation_end_2, end_1_day_41_norm_cap, end_1_day_41_long_cap,
                            end_1_day_41_short_cap, end_1_day_95_norm_cap, end_1_day_95_long_cap, 
                            end_1_day_95_short_cap, end_2_day_41_norm_cap, end_2_day_41_long_cap,
                            end_2_day_41_short_cap, end_2_day_95_norm_cap, end_2_day_95_long_cap,
                            end_2_day_95_short_cap, max_day_cap, min_day_cap, median_day_cap, max_night_cap,
                            min_night_cap, median_night_cap, max_norm_day_cap, min_norm_day_cap,
                            max_norm_night_cap, min_norm_night_cap, capacity_mw_summer, capacity_mw_winter)

                df_list.append(df_item)

            df = pd.DataFrame(df_list)
            cols = ["line_name", "line_name_1", "line_name_2", "reason_end_1", "reason_end_2", 
                    "start_date_end_1", "end_date_end_1", "start_date_end_2", "end_date_end_2", "start_date", "end_date", "status_end_1", 
                    "status_end_2", "end_1", "end_2", "dev_end_1", "dev_end_2", "kv_end_1", "kv_end_2",
                    "substation_end_1", "substation_end_2", "end_1_day_41_norm_cap", "end_1_day_41_long_cap",
                    "end_1_day_41_short_cap", "end_1_day_95_norm_cap", "end_1_day_95_long_cap", 
                    "end_1_day_95_short_cap", "end_2_day_41_norm_cap", "end_2_day_41_long_cap",
                    "end_2_day_41_short_cap", "end_2_day_95_norm_cap", "end_2_day_95_long_cap",
                    "end_2_day_95_short_cap", "max_day_cap", "min_day_cap", "median_day_cap", "max_night_cap",
                    "min_night_cap", "median_night_cap", "max_norm_day_cap", "min_norm_day_cap",
                    "max_norm_night_cap", "min_norm_night_cap", "capacity_mw_summer", "capacity_mw_winter"]
            df.columns = [x.upper() for x in cols]
            pks = ["LINE_NAME", "POSTED_DATE"]
            df["REASON_END_1"] = df["REASON_END_1"].str.strip()
            df["REASON_END_2"] = df["REASON_END_2"].str.strip()
            df["POSTED_DATE"] = datetime.strptime(posted_date, "%m-%d-%Y %H:%M:%S")
            df["INSERT_DATE"] = datetime.now()
            df["UPDATE_DATE"] = datetime.now()
            print("Length of dataframe before drop_duplicates for pjm_temp_line_ratings******",len(df))
            df.drop_duplicates(subset=pks,keep='first',inplace=True)
            print("Length of dataframe after drop_duplicates for pjm_temp_line_ratings********",len(df))
            df = df[['LINE_NAME', 'LINE_NAME_1', 'LINE_NAME_2', 'REASON_END_1', 'REASON_END_2', 'START_DATE_END_1', 'END_DATE_END_1', 'START_DATE_END_2', 'END_DATE_END_2', 'START_DATE', 'END_DATE', 'STATUS_END_1', 'STATUS_END_2', 'END_1', 'END_2', 'DEV_END_1', 'DEV_END_2', 'KV_END_1', 'KV_END_2', 'SUBSTATION_END_1', 'SUBSTATION_END_2', 'END_1_DAY_41_NORM_CAP', 'END_1_DAY_41_LONG_CAP', 'END_1_DAY_41_SHORT_CAP', 'END_1_DAY_95_NORM_CAP', 'END_1_DAY_95_LONG_CAP', 'END_1_DAY_95_SHORT_CAP', 'END_2_DAY_41_NORM_CAP', 'END_2_DAY_41_LONG_CAP', 'END_2_DAY_41_SHORT_CAP', 'END_2_DAY_95_NORM_CAP', 'END_2_DAY_95_LONG_CAP', 'END_2_DAY_95_SHORT_CAP', 'MAX_DAY_CAP', 'MIN_DAY_CAP', 'MEDIAN_DAY_CAP', 'MAX_NIGHT_CAP', 'MIN_NIGHT_CAP', 'MEDIAN_NIGHT_CAP', 'MAX_NORM_DAY_CAP', 'MIN_NORM_DAY_CAP', 'MAX_NORM_NIGHT_CAP', 'MIN_NORM_NIGHT_CAP', 'CAPACITY_MW_SUMMER', 'CAPACITY_MW_WINTER', 'UPDATE_DATE', 'INSERT_DATE', 'POSTED_DATE']]
            res = load_df_to_sf(df, databasename, schemaname, tablename, pks,uploadinsert=False)
        if res is not None:
            return res
        else:
            return 0
    except Exception as e:
        print("Exception caught during execution: ",e)
        logging.exception(f'Exception caught during execution: {e}')
        raise e

if __name__ == "__main__": 
    logging.info('Execution Started')
    starttime=datetime.now()
    emaildf = []
    try:
        credential_dict = get_config('PJMISO website data','PJM_LINE_RATINGS')
        databasename = credential_dict['DATABASE']
        schemaname = credential_dict['TABLE_SCHEMA']
        tablename = credential_dict['TABLE_NAME']
        print(databasename+schemaname+tablename)
        # url = "https://edart.pjm.com/reports/PJM_Line_ratings.txt"
        url = credential_dict['SOURCE_URL'].split(';')[0]
        print(url)
        json='[{"JOB_ID": '+str(job_id)+',"Current_DATE": "'+str(datetime.now())+'","databasename": "'+databasename+'","schemaname": "'+schemaname+'", "tablename":"'+tablename+'"}]'
        bu_alerts.bulog(process_name= tablename ,database= "POWERDB",status='Started',table_name=databasename+'.'+schemaname+'.'+tablename, row_count=0, log=json, warehouse='QUANT_WH',process_owner=credential_dict['IT_OWNER'])
        pjm_res = load_pjm_line_ratings(url, "", "", "PJM_Line_ratings",databasename,schemaname,tablename)
        emaildf_item = (tablename, pjm_res, datetime.now().strftime("%Y-%m-%d"))
        emaildf.append(emaildf_item)
        json='[{"JOB_ID": '+str(job_id)+',"Current_DATE": "'+str(datetime.now())+'","databasename": "'+databasename+'","schemaname": "'+schemaname+'", "tablename":"'+tablename+'"}]'
        bu_alerts.bulog(process_name= tablename ,database= "POWERDB",status='Completed',table_name=databasename+'.'+schemaname+'.'+tablename, row_count=pjm_res, log=json, warehouse='QUANT_WH',process_owner=credential_dict['IT_OWNER'])
        credential_dict = get_config('PJMISO website data','PJM_TEMP_LINE_RATINGS')
        databasename = credential_dict['DATABASE']
        schemaname = credential_dict['TABLE_SCHEMA']
        tablename = credential_dict['TABLE_NAME']
        print(databasename+schemaname+tablename)
        # url = "https://edart.pjm.com/reports/PJM_TEMP_Line_ratings.txt"
        url = credential_dict['SOURCE_URL'].split(';')[0]
        print(url)
        json='[{"JOB_ID": '+str(job_id)+',"Current_DATE": "'+str(datetime.now())+'","databasename": "'+databasename+'","schemaname": "'+schemaname+'", "tablename":"'+tablename+'"}]'
        bu_alerts.bulog(process_name= tablename ,database= "POWERDB",status='Started',table_name=databasename+'.'+schemaname+'.'+tablename, row_count=0, log=json, warehouse='QUANT_WH',process_owner=credential_dict['IT_OWNER'])
        temp_res = load_pjm_temp_line_ratings(url, "", "", "PJM_TEMP_Line_ratings",databasename,schemaname,tablename)
        emaildf_item = (tablename, temp_res, datetime.now().strftime("%Y-%m-%d"))
        emaildf.append(emaildf_item)
        json='[{"JOB_ID": '+str(job_id)+',"Current_DATE": "'+str(datetime.now())+'","databasename": "'+databasename+'","schemaname": "'+schemaname+'", "tablename":"'+tablename+'"}]'
        bu_alerts.bulog(process_name= tablename ,database= "POWERDB",status='Completed',table_name=databasename+'.'+schemaname+'.'+tablename, row_count=temp_res, log=json, warehouse='QUANT_WH',process_owner=credential_dict['IT_OWNER'])
                
        senddf = pd.DataFrame(emaildf, columns=["TABLE", "NUMBER OF ROWS", "INSERT DATE"])
        logging.info(f'Data Insertion Details \n\n {senddf}')
        logging.info('Execution Done')
        bu_alerts.send_mail(
            receiver_email = credential_dict['EMAIL_LIST'],
            mail_subject ='JOB SUCCESS - PJM_LINE_RATINGS & PJM_TEMP_LINE_RATINGS',
            mail_body = 'PJM_LINE_RATINGS & PJM_TEMP_LINE_RATINGS completed successfully, Attached logs',
            attachment_location = log_file_location
        )
    except Exception as e:
        print("Exception caught during execution: ",e)
        logging.exception(f'Exception caught during execution: {e}')
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'","databasename": "'+databasename+'","schemaname": "'+schemaname+'"}]'
        bu_alerts.bulog(process_name="PJM_LINE_RATINGS & PJM_TEMP_LINE_RATINGS",database='POWERDB',status='Failed',table_name= "", row_count=0, log=log_json, warehouse='QUANT_WH',process_owner=credential_dict['IT_OWNER']) 
        bu_alerts.send_mail(
            receiver_email = credential_dict['EMAIL_LIST'],
            mail_subject='JOB FAILED - PJM_LINE_RATINGS & PJM_TEMP_LINE_RATINGS',
            mail_body='PJM_LINE_RATINGS & PJM_TEMP_LINE_RATINGS failed during execution, Attached logs',
            attachment_location = log_file_location
        )
    endtime=datetime.now()
    print('Complete work at {} ...'.format(endtime.strftime('%Y-%m-%d %H:%M:%S')))
    print('Total time taken: {} seconds'.format((endtime-starttime).total_seconds()))

# %%