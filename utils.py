# -*- coding: utf-8 -*-
# @Author: Muhammad Alfin N
# @Date:   2022-08-09 11:38:44
# @Last Modified by:   aldomasendi
# @Last Modified time: 2022-08-09 11:54:29

import os
import json
import time
import pyodbc
import psycopg2
import openpyxl
import pandas as pd

from datetime import datetime, timedelta
from io import StringIO

def db_connect(db_access,jdbc='sql'):
    print('connecting to',jdbc)
    server = db_access['address']
    db = db_access['db']
    username= db_access['username']
    password= db_access['pass']
        
    if jdbc=='sql':
        conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+db+';UID='+username+';PWD='+ password)
        curr = conn.cursor()
        
    if jdbc=='postgres':
        conn = psycopg2.connect(host=server, port='5432', dbname=db, user=username, password=password)
        curr = conn.cursor()
    
    print('successfully connect to ',jdbc)
    return conn,curr

def parse_json(filename):
    
    file = open(os.path.join('map',filename))
    data = json.loads(file.read())
    
    return data

def parse_date(date,dimension='daily'):
    date = datetime.strptime(date,'%Y%m%d')
    
    if dimension=='daily':
        return date.strftime('%Y%m%d')
    
    if dimension=='weekly':
        return date.strftime('%Y%V')
    
    if dimension=='monthly':
        return date.strftime('%Y%m')
    
    return None

def ingest_to_pg(df,curr_pg,conn_pg,table):

    # Initialize a string buffer
    sio = StringIO()
    sio.write(df.to_csv(index=False,header=False, sep=','))  # Write the Pandas DataFrame as a csv to the buffer
    sio.seek(0)  # Be sure to reset the position to the start of the stream
    
    # Copy the string buffer to the database, as if it were an actual file
    sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
    curr_pg.copy_expert(sql=sql % table, file=sio)
    conn_pg.commit()
