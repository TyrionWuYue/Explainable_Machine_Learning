# Systhesis datasets
import pandas as pd
import os
from data_preprocess import *
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

# obtain all subdirectories under the dataset folder
def get_subdirectories(folder_path):
  subdirectories = [d for d in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, d))]
  return subdirectories

def proccess_every_month_data(folder_path, parent_directory, n):
  subdirectories_list = get_subdirectories(folder_path)
  for date_str in subdirectories_list:
    print(f"{date_str} Started...")
    add_comp_info(parent_directory, date_str, n)
  print("Finish Preprocessing!")

def main():
  current_file_path = os.path.abspath(__file__)
  parent_directory = os.path.dirname(os.path.dirname(current_file_path))
  data_path = os.path.join(parent_directory, 'dataset')
  n = 4
  process_every_month_data(data_path, parent_directory, n)

if __name__ == '__main__':
  main()
