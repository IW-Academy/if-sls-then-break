import pymysql
from os import environ
import time
from ETL.extract import Extract
from ETL.transform import Transform
from ETL.load import Load

def run_etl(filename):
    start = time.time()
    app = Extract()
    raw_data_list = app.get_data_from_bucket(filename) # extract output
    end_extract = time.time()
    extract_time = round(end_extract - start, 4)
    print(f"Extract time: {extract_time}")
    apple = Transform()
    transformed_data, transformed_drink_menu_data = apple.transform_new_data(raw_data_list) # raw data into transform returns transformed data and drinks dic

    end_transform = time.time()
    transform_time = round(end_transform - end_extract,4)
    print(f"Transform time: {transform_time}")
    appley = Load()

    appley.save_transaction(transformed_data) # populate RDS instance with cleaned data.
    appley.save_drink_menu(transformed_drink_menu_data) # generate drinks menu
 
    end_load = time.time()
    load_time = round(end_load - end_transform, 4)
    total_time = extract_time + transform_time + load_time
    print(f"Load time: {load_time}\nTotal time: {total_time}")

