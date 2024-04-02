

import pandas as pd
import numpy as np
import pandas_gbq
from google.cloud import bigquery

excel_file_path = '/content/etl_test_source.xlsx'
all_sheets_dict = pd.read_excel(excel_file_path, sheet_name=None)
print("Sheet names:", all_sheets_dict.keys())

"""INGESTION"""

csv_data_users = pd.read_excel(excel_file_path, sheet_name='users')
csv_data_users.to_csv('/content/data_users.csv',index=None,header =True)
csv_data_trx = pd.read_excel(excel_file_path, sheet_name='transactions', engine='openpyxl')
csv_data_trx.to_csv('/content/data_trx.csv',index=False)
csv_data_memp = pd.read_excel(excel_file_path, sheet_name='membership_purchases', engine='openpyxl')
csv_data_memp.to_csv('/content/data_memp.csv',index=False)
csv_data_mdr = pd.read_excel(excel_file_path, sheet_name='mdr_data', engine='openpyxl')
csv_data_mdr.to_csv('/content/data_mdr.csv',index=False)
csv_data_activ = pd.read_excel(excel_file_path, sheet_name='user_activity', engine='openpyxl')
csv_data_activ.to_csv('/content/data_activ.csv',index=False)

df_users = pd.read_csv('/content/data_users.csv', quotechar='"', quoting=3, doublequote=True, engine='python' )
df_users = df_users.replace(r'"', '', regex=True)
df_users.rename(columns=lambda x: x.replace('"', ''), inplace=True)
df_trx = pd.read_csv('/content/data_trx.csv', quotechar='"', quoting=3, doublequote=True, engine='python')
df_trx = df_trx.replace(r'"', '', regex=True)
df_trx.rename(columns=lambda x: x.replace('"', ''), inplace=True)
df_memp = pd.read_csv('/content/data_memp.csv', quotechar='"', quoting=3, doublequote=True, engine='python')
df_memp = df_memp.replace(r'"', '', regex=True)
df_memp.rename(columns=lambda x: x.replace('"', ''), inplace=True)
df_mdr = pd.read_csv('/content/data_mdr.csv', quotechar='"', quoting=3, doublequote=True, engine='python')
df_mdr = df_mdr.replace(r'"', '', regex=True)
df_mdr.rename(columns=lambda x: x.replace('"', ''), inplace=True)
df_activ = pd.read_csv('/content/data_activ.csv', quotechar='"', quoting=3, doublequote=True, engine='python')
df_activ = df_activ.replace(r'"', '', regex=True)
df_activ.rename(columns=lambda x: x.replace('"', ''), inplace=True)

"""NULL HANDLING"""

df_users.replace('', np.nan, inplace=True)
df_users.replace(np.nan, 'NOT AVAILABLE', inplace=True)
df_trx.replace(np.nan, 0,inplace=True)
df_memp.replace(np.nan, 'Basic',inplace=True)

df_users

"""DATE TIME HANDLING"""

df_trx['Timestamp'] = pd.to_datetime(df_trx['Timestamp'])
df_trx['Date'] = df_trx['Timestamp'].dt.date
df_trx['Time'] = df_trx['Timestamp'].dt.time
df_trx['Date'] = pd.to_datetime(df_trx['Date'])

df_trx

df_memp['Purchase_Date'] = pd.to_datetime(df_memp['Purchase_Date'] )

df_memp['Expiry_Date'] = pd.to_datetime(df_memp['Expiry_Date'] )

df_activ['Activity_Date'] = pd.to_datetime(df_activ['Activity_Date'] )

df_activ.dtypes

df_mdr = df_mdr.drop(index=2,axis=0)
df_mdr = df_mdr.drop(index=3,axis=0)
df_mdr



dataset_id = 'sprout_testground'
table_id = 'df_users'
project_id = 'bigquerydev-303605'

# Upload DataFrame to BigQuery
pandas_gbq.to_gbq(df_users, f'{dataset_id}.{table_id}', project_id=project_id, if_exists='replace')

dataset_id = 'sprout_testground'
table_id = 'df_activ'
project_id = 'bigquerydev-303605'

# Upload DataFrame to BigQuery
pandas_gbq.to_gbq(df_activ, f'{dataset_id}.{table_id}', project_id=project_id, if_exists='replace')

dataset_id = 'sprout_testground'
table_id = 'df_mdr'
project_id = 'bigquerydev-303605'

# Upload DataFrame to BigQuery
pandas_gbq.to_gbq(df_mdr, f'{dataset_id}.{table_id}', project_id=project_id, if_exists='replace')

dataset_id = 'sprout_testground'
table_id = 'df_memp'
project_id = 'bigquerydev-303605'

# Upload DataFrame to BigQuery
pandas_gbq.to_gbq(df_memp, f'{dataset_id}.{table_id}', project_id=project_id, if_exists='replace')

df_trx.dtypes

df_trx['Timestamp'] = df_trx['Timestamp'].astype(str)

df_trx.to_csv('/content/df_trx.csv',index)

dataset_id = 'sprout_testground'
table_id = 'df_trx'
project_id = 'bigquerydev-303605'

schema = [
    {'name': 'Transaction_ID', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    {'name': 'User_ID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'Transaction_Amount', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    {'name': 'Timestamp', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'Date', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'Time', 'type': 'STRING', 'mode': 'NULLABLE'},
]

# Upload DataFrame to BigQuery
pandas_gbq.to_gbq(df_trx, f'{dataset_id}.{table_id}', project_id=project_id, if_exists='replace',table_schema=schema)