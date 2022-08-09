# -*- coding: utf-8 -*-
# @Author: Muhammad Alfin N
# @Date:   2022-08-09 11:50:47
# @Last Modified by:   aldomasendi
# @Last Modified time: 2022-08-09 12:02:11

import os
import json
import pandas as pd
import utils
import config

from datetime import datetime

# ==============================================
# Date To Ingest

# where datetime >= GETDATE() - 3
# ==============================================

select_query = '''
        SELECT {}
        FROM your_db.schema.{}
        where {} >= GETDATE() - 3
    '''

delete_query = '''
        DELETE
        FROM schema_on_pg.{}
        WHERE {} >= current_date - 3
    '''

# loop if more than one
source = config.db_source()[0]

# access
db_access = source['db_access']
db_table = source['table']
db_name = source['name']

# Connect to sql_server
conn,curr = utils.db_connect(db_access)

# connect to postgres
conn_pg,curr_pg = utils.db_connect({
            'address' : '127.0.0.1',
            'username': 'userpg',
            'pass': 'userpasspg',
            'db': 'dbonpg',
            },'postgres')

for i in range(len(db_table)):
        table = utils.parse_json(db_table[i])

        # read data from mssql
        print('getting',table['name'][0])
        df = pd.read_sql(select_query.format(table['column'],table['name'][0],table['date_column']),conn)
        print(df.head())

        # Deleting Data In Postgre
        print('deleting schema_on_pg.{} in postgres'.format(table['name'][1]))
        curr_pg.execute(delete_query.format(table['name'][1],table['date_column']))
        conn_pg.commit()

        # Ingest Data To Postgre
        print('inserting to schema_on_pg.{}'.format(table['name'][1]))
        utils.ingest_to_pg(df,curr_pg,conn_pg,"schema_on_pg.{}".format(table['name'][1]))
        print('Done inserting',table['name'][0])

print('Done Ingest to Postgres...')
curr.close()
conn.close()
curr_pg.close()
conn_pg.close()