# Data Preprocessing

import pandas as pd
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

def get_last_day_of_month(date_str):
# The format of input value is '23Aug', '24Jan', etc.
# Output: 2023-08-31, 2024-01-31, etc.
  year_str = '20' + date_str[:2]
  date_object = datetime.strptime(date_str, '%d%b')
  first_day_of_next_month = datetime(date_object.year, date_object.month % 12 + 1, 1)
  last_day_of_current_month = first_day_of_next_month - timedelta(days = 1)
  last_day_object = datetime(int(year_str), last_day_of_current_month.month, last_day_of_current_month.day)
  return last_day_object.strftime('%Y-%m-%d')


def get_wide_data(parent_directory, date_str, n):
  last_day = get_last_day_of_month(date_str)
  T = datetime.strptime(last_day, '%Y-%m-%d')
  every_month_path = os.path.join(parent_directory, f'dataset/{date_str}/every_month.xlsx')

  # Pivot
  df = pd.read_excel(every_month_path, dtype={'user_key':str, 'event_day':str})
  indicators = df.columns.to_list()[2:]
  df = df.dropna(how='all')
  df = df.drop_duplicates(subset=['user_key', 'event_day'])
  df = df.pivot(index='user_key', columns='event_day', values=indicators)
  column_list = ["_".join(x) for x in df.columns]
  df.columns = column_list
  df = df.reset_index()
  df = df.fillna(value=0)

  # Rename the columns
  tmp = T - relativedelta(months=n-1)
  time_list = []
  for i in range(13 - n):
    tmp = tmp - relativedelta(months=1)
    time_list.append(tmp.strftime(%Y%m))
  column_list = []
  for column in df.columns:
    x = 0
    for i in range(len(time_list)):
      if column.endwith(time_list[i]):
        first = column[:-7]
        second = i + n
        column_list.append('%s_T-%d' % (first, second))
        x = 1
        break
      if x == 0:
        column_list.append(column)
  df.columns = column_list
  return df

def get_df(parent_directory, date_str, n):
  df_em = get_wide_data(parent_directory, date_str, n)
  if_renew_path = os.path.join(parent_directory, f'dataset/{date_str}/if_renew.xlsx')
  df_od = pd.read_excel(if_renew_path, dtype={'user_key':str})
  df_od = df_od.drop_duplicates(subset=['user_key'])
  df_merged = pd.merge(df_em, df_od, on='user_key', how='inner')
  return df_merged

def add_comp_info(parent_directory, date_str='23Nov', n=4):
  comp_info_cols = ['xxx'] # sensetive data
  df_merged = get_df(parent_directory, date_str, n)
  comp_info_path = os.path.join(parent_directory, f'dataset/{date_str}/{date_str}_comp_info.csv')
  comp_info = pd.read_csv(comp_info_path, dtype={'user_key':str})
  comp_info['user_key'] = comp_info['user_key'].fillna(0).astype(float).astype(int).astype(str)
  for i in comp_info_cols:
    if i not in list(comp_info.columns):
      comp_info[i] = 0
  comp_info = comp_info[comp_info_cols]
  comp_info = comp_info.drop_duplicates()
  df = pd.merge(df_merged, comp_info, on='user_key', how='inner')
  df = df.drop_duplicates(subset=['user_key'])
  df['expire_month'] = date_str
  if len(df) != len(df_merged):
    raise ValueError(os.path.join(parent_directory, f'dataset/{date_str}/{date_str}_data.csv'), index=False)

def main():
  current_file_path = os.path.abspath(__file__)
  parent_directory = os.path.dirname(os.path.dirname(current_file_path))
  add_comp_info(parent_directory)

if __name__ == '__main__':
  main()
              
  
