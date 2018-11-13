import os
import sqlite3
import sys

import pandas as pd

import settings as s

if os.path.exists(s.filename_database):
    sys.stderr.write('* ERROR *: {:} already exists\n'.format(s.filename_database))
    sys.exit()

print('--- Reading "{:}" source data'.format(s.filename_sourcedata))
df = pd.read_csv(s.filename_sourcedata)

print('--- Creating database "{:}"'.format(s.filename_database))
conn = sqlite3.connect(s.filename_database)

# Create table of fully populated training data
print('--- Creating "{:}" table for base training data'.format(s.db_table_name_training))
df_base_training = df[df[s.field_date] <= s.latest_date_of_base_training_data].copy()
df_base_training.to_sql(s.db_table_name_training, conn, index=False)

# Create table of fully population future data (for simulation)
print('--- Creating "{:}" table for future unknown data'.format(s.db_table_name_future_unknown))
df_future_data = df[df[s.field_date] > s.latest_date_of_base_training_data].copy()
df_future_data.to_sql(s.db_table_name_future_unknown, conn, index=False)

# For template data (rows needed for forecast rows), remove all non-known in advance
print('--- Creating "{:}" table for future known data'.format(s.db_table_name_future_only_knowns))
columns_to_drop = []
for column in df_future_data.columns:
    if not(column == s.field_date or column in s.fields_known_in_advance):
        columns_to_drop.append(column)
df_future_only_knowns = df_future_data.drop(columns=columns_to_drop)
df_future_only_knowns.to_sql(s.db_table_name_future_only_knowns, conn, index=False)
