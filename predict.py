from io import StringIO
import configparser
import datetime
import sqlite3

import pandas as pd
import requests

import settings as s

# Get server and authentication details
config = configparser.ConfigParser()
config.read('batch_scoring.ini')

host = config['batch_scoring']['host']
datarobot_key = config['batch_scoring']['datarobot_key']
user = config['batch_scoring']['user']
api_token = config['batch_scoring']['api_token']

# Establish connection to database
conn = sqlite3.connect(s.filename_database)

# Retrieve latest model identified as best model
df = pd.read_sql('select * from {:}'.format(s.db_table_name_model_results), conn)
latest_model_info = df.sort_values(by='record_datetime').iloc[-1]
project_id = latest_model_info['project_id']
model_id = latest_model_info['deploy_model_id']

max_available_training_date = pd.read_sql('SELECT MAX(Date) FROM {:}'.format(s.db_table_name_training), conn).iloc[0, 0]
max_available_training_date = datetime.datetime.strptime(max_available_training_date, '%Y-%m-%d')
max_available_training_date

days_back = int(s.feature_derivation_window_start * 1.5)
td_days_back = datetime.timedelta(days=days_back)
days_forward = int(s.forecast_window_end * 1.5)
td_days_forward = datetime.timedelta(days=days_forward)

earliest_date = (max_available_training_date + td_days_back).isoformat()
latest_date = (max_available_training_date + td_days_forward).isoformat()

sql_training_rows = 'select * from {:} where {:} between "{:}" and "{:}"'.format(
    s.db_table_name_training,
    s.field_date,
    earliest_date,
    max_available_training_date.isoformat())
sql_template_rows = 'select * from {:} where {:} between "{:}" and "{:}"'.format(
    s.db_table_name_future_only_knowns,
    s.field_date,
    max_available_training_date.isoformat(),
    latest_date)

historical_rows = pd.read_sql(sql_training_rows, conn)
template_rows = pd.read_sql(sql_template_rows, conn)
scoring_rows = pd.concat([historical_rows, template_rows], ignore_index=True, sort=False)

scoring_data = StringIO()
scoring_rows.to_csv(scoring_data, index=False)
scoring_str = scoring_data.getvalue()

# Set HTTP headers
# Note: The charset should match the contents of the file.
headers = {'Content-Type': 'text/plain; charset=UTF-8', 'datarobot-key': datarobot_key}

predictions_response = requests.post('{:}/predApi/v1.0/{:}/{:}/timeSeriesPredict'.format(
                                        host, project_id, model_id),
                                     auth=(user, api_token), data=scoring_str,
                                     headers=headers,
                                     params={'forecastPoint': max_available_training_date.isoformat()})

df_predictions = pd.DataFrame(predictions_response.json()['data']).drop(columns=['predictionValues'])
df_predictions['datetime_recorded'] = datetime.datetime.now().isoformat()
df_predictions['project_id'] = project_id
df_predictions['model_id'] = model_id

df_predictions.to_sql(s.db_table_name_predictions, conn, index=False)
