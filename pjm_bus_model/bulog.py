# -*- coding: utf-8 -*-
"""
Created on Fri Apr 10 17:33:00 2020

@author: Michael.Huang
"""


import pandas as pd
import numpy as np
import datetime as dt
import socket
import json

import snowflake.connector
import logging


import email.message
import smtplib

#
# logging.basicConfig(level=logging.INFO)
logging.basicConfig(level=logging.DEBUG)

#warehouse='VIRTUALS_WH'


#%%

def get_connection():
    try:
        conn=snowflake.connector.connect(
            user='RADHA',
            password='Krishna@123',
            account='OS54042.east-us-2.azure',
            warehouse='ITPYTHON_WH',
            database='POWERDB_DEV',
            schema='ISO',
            role='OWNER_POWERDB_DEV'
        )
        return conn
    except:
        logging.error('Cannot connect to snowflake')        
        return None
    
# get ptp awards
def bulog(process_name, status, table_name, row_count, log, warehouse,process_owner):
    try:
        log2=json.loads(log)
    except ValueError:
        log='[{"Error":"Invalid JSON String"}]'
        
    sql='''
        insert into powerdb.plog.process_log 
        select column1,column2,column3,column4,column5,column6, parse_json(column7), column8,column9
        from values('{}','{}','{}','{}','{}','{}','{}','{}','{}')
        '''
    processed_by=socket.gethostname()
    starttime=dt.datetime.now()
    
    try:
        sql=sql.format(process_name, processed_by, starttime, status, table_name, row_count, log, warehouse,process_owner)
        conn=get_connection()
        conn.cursor().execute('USE WAREHOUSE {}'.format(warehouse))
        conn.cursor().execute(sql)
        conn.close()
    except:
        pass
    
def bumail(to_addr_list, subject, message, from_addr='power@biourja.com'):
    
    msg = email.message.Message()
    msg['Subject'] = 'BULOG: '+subject
    msg['From'] = from_addr
    
    msg.add_header('Content-Type','text/html')
    
    msg.set_payload(message)
    
    s = smtplib.SMTP(host='us-smtp-outbound-1.mimecast.com', port=587)
    s.starttls()
    s.login('virtual-out@biourja.com', 't?%;`p39&Pv[L<6Y^cz$z2bn')
    s.sendmail(msg['From'], to_addr_list, msg.as_string())
    s.quit()
    
    
def bulog2(conn, process_name, status, table_name, row_count, log, warehouse):
    try:
        log2=json.loads(log)
    except ValueError:
        log='[{"Error":"Invalid JSON String"}]'
        
    sql='''
       insert into powerdb.plog.process_log 
        select column1,column2,column3,column4,column5,column6, parse_json(column7), column8
        from values('{}','{}','{}','{}','{}','{}','{}','{}')
        '''
    processed_by=socket.gethostname()
    starttime=dt.datetime.now()
    
    try:
        sql=sql.format(process_name, processed_by, starttime, status, table_name, row_count, log, warehouse)
        conn.cursor().execute('USE WAREHOUSE {}'.format(warehouse))
        conn.cursor().execute(sql)
    except:
        pass
#%%
# test bulog
#bulog('Test_Script.py','Started','table_x',123,'{"a":1,"b":2}','VIRTUALS_WH')

#%%
#test bumail
#bumail(to_addr_list=['michael.huang@biourja.com'], from_addr='virtuals-dev@biourja.com',subject='test subject',message='test message')
        