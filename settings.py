filename_database = 'database.db'
filename_sourcedata = 'DR_Demo_Store_Sales_Forecast_Train.csv'

field_date = 'Date'

# assign to None if there are no Known In Advance features
fields_known_in_advance = ['DestinationEvent', 'Marketing',
                           'Near_BlackFriday', 'Holiday', 'Near_Xmas']

feature_derivation_window_start = -28
feature_derivation_window_end = 0
forecast_window_start = 1
forecast_window_end = 7
validation_length = None # format 'P0Y0M0DT0H0M0S'
gap_length = None # format 'P0Y1M0DT0H0M0S'

latest_date_of_base_training_data = '2014-03-31'

db_table_name_training = 'training'
db_table_name_model_results = 'model_results'
db_table_name_predictions = 'predictions'

# will have known in advance populated
db_table_name_future_only_knowns = 'future_only_knowns'

# place for labeled future data for simulation
db_table_name_future_unknown = 'future_unknown'

datarobot_project_basename = 'simulated_ts_tr/pr/re'
number_of_backtests = 3
max_number_of_rows_for_project = 600 # will only build project of latest 600 rows

fields_exclude_from_modeling = []

target_name = 'Sales'
worker_count = 20

ranking_metric = 'RMSE'
sorting_reversed = False # True if higher is better, False if lower is better
