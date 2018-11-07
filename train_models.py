import datetime
import os
import sqlite3
import sys

import datarobot as dr
from datarobot import FeatureSettings
import pandas as pd

import settings as s

# # Connect to DataRobot
# connect to DataRobot, only if ~/.config/datarobot/drconfig.yaml doesn't exist already
# dr.Client(token='tokenfrom https://app.datarobot.com/account/me', endpoint='https://app.datarobot.com/api/v2')

# Read database
conn = sqlite3.connect(s.filename_database)

max_available_training_date = pd.read_sql('SELECT MAX(Date) FROM {:}'.format(s.db_table_name_training), conn).iloc[0, 0]
project_data = pd.read_sql(
        'SELECT * FROM {:} ORDER BY {:} DESC LIMIT {:}'.format(
            s.db_table_name_training, s.field_date, s.max_number_of_rows_for_project),
        conn)

project_name = s.datarobot_project_basename + '_' + max_available_training_date
print('--- Uploading "{:}" source data to create "{:}"'.format(s.filename_sourcedata, project_name))
project = dr.Project.create(project_data, project_name, max_wait=9999)
print('    {:}'.format(project.get_leaderboard_ui_permalink()))

# set up time series partition settings
if s.fields_known_in_advance:
    feature_settings = [ FeatureSettings( feat_name, known_in_advance=True ) for feat_name in s.fields_known_in_advance ]
else:
    feature_settings = None

time_partition = dr.DatetimePartitioningSpecification( 
    datetime_partition_column=s.field_date,
    use_time_series=True,
    feature_derivation_window_start=s.feature_derivation_window_start,
    feature_derivation_window_end=s.feature_derivation_window_end,
    validation_duration=s.validation_length,
    gap_duration=s.gap_length,
    forecast_window_start=s.forecast_window_start,
    forecast_window_end=s.forecast_window_end,
    number_of_backtests=s.number_of_backtests,
    feature_settings=feature_settings
    )
    
# Create a feature list from Informative Features excluding from Feature to remove
fl_informative_features = [fl for fl in project.get_featurelists() if fl.name == 'Informative Features'][0]
if s.fields_exclude_from_modeling:
    subset_of_features = list(set(fl_informative_features.features) - set(s.fields_exclude_from_modeling))
    new_featurelist = project.create_featurelist('Custom Base Features', subset_of_features)
else:
    new_featurelist = fl_informative_features
    
# start autopilot
print('--- Starting Autopilot')
project.set_target(target=s.target_name,
                   worker_count=s.worker_count,
                   partitioning_method=time_partition,
                   featurelist_id=new_featurelist.id,
                   max_wait=1800)
project.wait_for_autopilot(timeout=9999)

# Find best model, and write results back into database
print('--- Choosing the best model based on {:} in backtesting'.format(s.ranking_metric))
models = project.get_datetime_models()
best_model = sorted([m for m in models if m.metrics[s.ranking_metric]['backtesting']],
       key=lambda m: m.metrics[s.ranking_metric]['backtesting'],
       reverse=s.sorting_reversed)[0]
values = {
          'project_creation_date': project.created.isoformat(),
          'project_id': project.id,
          'base_model_id': best_model.id,
          'base_model_url': best_model.get_leaderboard_ui_permalink(),
          'model_type': best_model.model_type,
          'model_category': best_model.model_category,
          'processes': '|'.join(best_model.processes),
          'metric': s.ranking_metric,
          'validation': best_model.metrics[s.ranking_metric]['validation'],
          'backtesting': best_model.metrics[s.ranking_metric]['backtesting'],
         }

# Unlock holdout and train on most recent data with frozen model
print('--- Retraining best model into holdout')
project.unlock_holdout()
frozen_model_job = best_model.request_frozen_datetime_model()
model_to_deploy = frozen_model_job.get_result_when_complete(max_wait=9999)

values['deploy_model_id'] = model_to_deploy.id
values['deploy_model_url'] = model_to_deploy.get_leaderboard_ui_permalink()
values['record_datetime'] = datetime.datetime.now().isoformat()

print('--- Writing results to {:}'.format(s.db_table_name_model_results))
results = pd.DataFrame([values])
results.to_sql(s.db_table_name_model_results, conn, if_exists='append')
