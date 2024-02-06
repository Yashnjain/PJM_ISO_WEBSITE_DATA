#%%

import sys
from bu_snowflake import get_connection
from tosnowflake import get_max_date, load_df_to_sf, sendmail
import requests
import json
import re
import pandas as pd
from datetime import datetime
#%%

def load_pjm_temp_line_ratings(url, username, password, filename, databasename, schemaname, tablename):
    r = requests.get(url)
    content = r.content.decode('UTF-8').split("Substn")
    for i in range(1, len(content)):
        content[i] = "Substn" + content[i]

    posted_date = content[0].split("Posted at ")[1][:10]
    posted_date = posted_date[6:] + "-" + posted_date[:5] 
    content = content[1:]
    df_list = []

    try:
        max_date = get_max_date(databasename, schemaname, tablename, "POSTED_DATE")
        max_date = max_date.astype(str)[:10]
        max_date = datetime.strptime(max_date, "%Y-%m-%d")

        if max_date >= datetime.strptime(posted_date, "%Y-%m-%d"):
            conn = get_connection()
            conn.cursor().execute("use warehouse quant_wh")
            conn.cursor().execute("use database {}".format(databasename))
            conn.cursor().execute("delete from {} where posted_date = '{}'".format(schemaname+"."+tablename, posted_date))
            print("Rows for {} removed.".format(posted_date))

    except Exception as e:
        print(e)

    substn_a = [x for x in content if "End: END A" in x or "End: LOW" in x]
    substn_b = [x for x in content if "End: END B" in x or "End: HIGH" in x]

    for substn in substn_a:
        substn = substn.split("\n")
        line_name = re.sub(r"\s|Substn:|kV|Dev|End", "", substn[0])
        line_name = re.sub(":", "_", line_name)[:-5]
        line_name_1 = re.sub("\r", "", re.sub(r"\s{2,}", " ", substn[1]).split(": ")[1])
        reason_end_1 = substn[2].split(": ")[1]
        info = substn[3].split(": ")
        start_date_end_1 = datetime.strptime(info[1][:-8], "%m/%d/%Y %H:%M")
        end_date_end_1 = datetime.strptime(info[2][:-7], "%m/%d/%Y %H:%M")
        status_end_1 = info[-1][:-1]
        end_1 = "END A"
        dev_end_1 = line_name.split("_")[2]
        kv_end_1 = line_name.split("_")[1][:-2] + " " + line_name.split("_")[1][-2:]
        substation_end_1 = line_name.split("_")[0]
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
            median_day_cap = (all_day[mid] + all_day[~mid]) // 2
            max_night_cap = all_night[-1]
            min_night_cap = all_night[0]
            mid = len(all_night) // 2
            median_night_cap = (all_night[mid] + all_night[~mid]) // 2
            max_norm_day_cap = max(norm_day)
            min_norm_day_cap = min(norm_day)
            max_norm_night_cap = max(norm_night)
            min_norm_night_cap = min(norm_night)
            capacity_mw_summary = end_1_day_95_norm_cap
            capacity_mw_winter = end_1_day_41_norm_cap
        
        else:
            line_name_2 = re.sub("\r", "", re.sub(r"\s{2,}", " ", match[1]).split(": ")[1])
            reason_end_2 = match[2].split(": ")[1]
            info = match[3].split(": ")
            start_date_end_2 = datetime.strptime(info[1][:-8], "%m/%d/%Y %H:%M")
            end_date_end_2 = datetime.strptime(info[2][:-7], "%m/%d/%Y %H:%M")
            status_end_2 = info[-1][:-1]
            end_1 = "END B"
            dev_end_2 = line_name.split("_")[2]
            kv_end_2 = line_name.split("_")[1][:-2] + " " + line_name.split("_")[1][-2:]
            substation_end_2 = line_name.split("_")[0]
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
            median_day_cap = (all_day[mid] + all_day[~mid]) // 2
            max_night_cap = all_night[-1]
            min_night_cap = all_night[0]
            mid = len(all_night) // 2
            median_night_cap = (all_night[mid] + all_night[~mid]) // 2
            max_norm_day_cap = max(norm_day)
            min_norm_day_cap = min(norm_day)
            max_norm_night_cap = max(norm_night)
            min_norm_night_cap = min(norm_night)
            capacity_mw_summary = end_1_day_95_norm_cap
            capacity_mw_winter = end_1_day_41_norm_cap

        df_item = (line_name, line_name_1, line_name_2, reason_end_1, reason_end_2, 
                    start_date_end_1, end_date_end_1, start_date_end_2, end_date_end_2, 
                    status_end_1, status_end_2, end_1, end_2, dev_end_1, dev_end_2, kv_end_1, kv_end_2,
                    substation_end_1, substation_end_2, end_1_day_41_norm_cap, end_1_day_41_long_cap,
                    end_1_day_41_short_cap, end_1_day_95_norm_cap, end_1_day_95_long_cap, 
                    end_1_day_95_short_cap, end_2_day_41_norm_cap, end_2_day_41_long_cap,
                    end_2_day_41_short_cap, end_2_day_95_norm_cap, end_2_day_95_long_cap,
                    end_2_day_95_short_cap, max_day_cap, min_day_cap, median_day_cap, max_night_cap,
                    min_night_cap, median_night_cap, max_norm_day_cap, min_norm_day_cap,
                    max_norm_night_cap, min_norm_night_cap, capacity_mw_summary, capacity_mw_winter)

        df_list.append(df_item)

    for substn in substn_b:
        substn = substn.split("\n")
        line_name = re.sub(r"\s|Substn:|kV|Dev|End", "", substn[0])
        line_name = re.sub(":", "_", line_name)[:-5]
        line_name_2 = re.sub("\r", "", re.sub(r"\s{2,}", " ", substn[1]).split(": ")[1])
        reason_end_2 = substn[2].split(": ")[1]
        info = substn[3].split(": ")
        start_date_end_2 = datetime.strptime(info[1][:-8], "%m/%d/%Y %H:%M")
        end_date_end_2 = datetime.strptime(info[2][:-7], "%m/%d/%Y %H:%M")
        status_end_2 = info[-1][:-1]
        end_1 = "END B"
        dev_end_2 = line_name.split("_")[2]
        kv_end_2 = line_name.split("_")[1][:-2] + " " + line_name.split("_")[1][-2:]
        substation_end_2 = line_name.split("_")[0]
        end_2_day_41 = [x for x in substn[12].split(" ") if len(x) > 0]
        end_2_day_41_norm_cap = int(end_2_day_41[1])
        end_2_day_41_long_cap = int(end_2_day_41[2])
        end_2_day_41_short_cap = int(end_2_day_41[3])
        end_2_day_95 = [x for x in substn[6].split(" ") if len(x) > 0]
        end_2_day_95_norm_cap = int(end_2_day_95[1])
        end_2_day_95_long_cap = int(end_2_day_95[2])
        end_2_day_95_short_cap = int(end_2_day_95[3])

        line_name_1 = None
        reason_end_2 = None
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
        median_day_cap = (all_day[mid] + all_day[~mid]) // 2
        max_night_cap = all_night[-1]
        min_night_cap = all_night[0]
        mid = len(all_night) // 2
        median_night_cap = (all_night[mid] + all_night[~mid]) // 2
        max_norm_day_cap = max(norm_day)
        min_norm_day_cap = min(norm_day)
        max_norm_night_cap = max(norm_night)
        min_norm_night_cap = min(norm_night)
        capacity_mw_summary = end_2_day_95_norm_cap
        capacity_mw_winter = end_2_day_41_norm_cap

        df_item = (line_name, line_name_1, line_name_2, reason_end_1, reason_end_2, 
                    start_date_end_1, end_date_end_1, start_date_end_2, end_date_end_2, 
                    status_end_1, status_end_2, end_1, end_2, dev_end_1, dev_end_2, kv_end_1, kv_end_2,
                    substation_end_1, substation_end_2, end_1_day_41_norm_cap, end_1_day_41_long_cap,
                    end_1_day_41_short_cap, end_1_day_95_norm_cap, end_1_day_95_long_cap, 
                    end_1_day_95_short_cap, end_2_day_41_norm_cap, end_2_day_41_long_cap,
                    end_2_day_41_short_cap, end_2_day_95_norm_cap, end_2_day_95_long_cap,
                    end_2_day_95_short_cap, max_day_cap, min_day_cap, median_day_cap, max_night_cap,
                    min_night_cap, median_night_cap, max_norm_day_cap, min_norm_day_cap,
                    max_norm_night_cap, min_norm_night_cap, capacity_mw_summary, capacity_mw_winter)

        df_list.append(df_item)

    df = pd.DataFrame(df_list)
    cols = ["line_name", "line_name_1", "line_name_2", "reason_end_1", "reason_end_2", 
            "start_date_end_1", "end_date_end_1", "start_date_end_2", "end_date_end_2", "status_end_1", 
            "status_end_2", "end_1", "end_2", "dev_end_1", "dev_end_2", "kv_end_1", "kv_end_2",
            "substation_end_1", "substation_end_2", "end_1_day_41_norm_cap", "end_1_day_41_long_cap",
            "end_1_day_41_short_cap", "end_1_day_95_norm_cap", "end_1_day_95_long_cap", 
            "end_1_day_95_short_cap", "end_2_day_41_norm_cap", "end_2_day_41_long_cap",
            "end_2_day_41_short_cap", "end_2_day_95_norm_cap", "end_2_day_95_long_cap",
            "end_2_day_95_short_cap", "max_day_cap", "min_day_cap", "median_day_cap", "max_night_cap",
            "min_night_cap", "median_night_cap", "max_norm_day_cap", "min_norm_day_cap",
            "max_norm_night_cap", "min_norm_night_cap", "capacity_mw_summary", "capacity_mw_winter"]
    df.columns = [x.upper() for x in cols]
    df["POSTED_DATE"] = datetime.strptime(posted_date, "%Y-%m-%d")
    pks = ["LINE_NAME", "POSTED_DATE"]

    return load_df_to_sf(df, databasename, schemaname, tablename, pks)
# %%

url = "https://edart.pjm.com/reports/PJM_TEMP_Line_ratings.txt"
to_addr = ["sandeep.singh@biourja.com", 
           "amrish.asokan@biourja.com", "deeksha.tiwari@biourja.com"]
res = load_pjm_temp_line_ratings(url, "", "", "PJM_TEMP_Line_ratings", "POWERDB_BIZDEV", "PTEST", "PJM_TEMP_LINE_RATINGS")
if res is not None:
    sendmail(to_addr, "PJM Line Ratings -> Snowflake",
             "{} rows uploaded.".format(res))
#%%