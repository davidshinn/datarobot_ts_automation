import datetime
import os
import sqlite3
import sys

import pandas as pd

import settings as s

# # Connect to DataRobot
# connect to DataRobot, only if ~/.config/datarobot/drconfig.yaml doesn't exist already
# dr.Client(token='tokenfrom https://app.datarobot.com/account/me', endpoint='https://app.datarobot.com/api/v2')

conn = sqlite3.connect(s.filename_database)

first_date_in_future = pd.read_sql('SELECT MIN(Date) FROM {:}'.format(s.db_table_name_future_unknown), conn).iloc[0, 0]
first_date_in_future

# Retrieve next chronilogical date from the future records
df = pd.read_sql('SELECT * FROM {:} WHERE {:} = "{:}"'.format(
    s.db_table_name_future_unknown, s.field_date, first_date_in_future), conn)

# Insert the records into the existing training data (as if actuals were recorded)
df.to_sql(s.db_table_name_training, conn, if_exists='append', index=False)

# Remove the records with the same dates from both future records
sql_delete = 'DELETE FROM {:} WHERE {:} = "{:}"'
conn.execute(sql_delete.format(s.db_table_name_future_unknown, s.field_date, first_date_in_future))
conn.execute(sql_delete.format(s.db_table_name_future_only_knowns, s.field_date, first_date_in_future))
conn.commit()
