from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.decorators import task

from datetime import datetime, timedelta
import pandas as pd
import glob
import zipfile
import os
from decouple import config
import psycopg2
from sqlalchemy import create_engine

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 5),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

def get_connection():
    conn = psycopg2.connect(
        dbname=config('DB_NAME'),
        user=config('DB_USER'),
        password=config('DB_PASSWORD'),
        host=config('DB_HOST')
    )
    return conn

def create_filenames_table_if_not_exists():
    conn = get_connection()
    cursor = conn.cursor()
    
    # Check if the filenames table exists
    cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'filenames')")
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        # If the table doesn't exist, create it
        cursor.execute("""
            CREATE TABLE filenames (
                id serial PRIMARY KEY,
                filename text,
                flag integer
            );
        """)

    conn.commit()
    conn.close()

def create_raw_data_table_if_not_exists():
    conn = get_connection()
    cursor = conn.cursor()
    
    # Check if the raw_data table exists
    cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'raw_data')")
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        # If the table doesn't exist, create it
        cursor.execute("""
            CREATE TABLE raw_data (
                id serial PRIMARY KEY,
                city text,
                type text,
                squareMeters real,
                rooms integer,
                floor integer,
                floorCount integer,
                buildYear integer,
                latitude real,
                longitude real,
                centreDistance real,
                poiCount integer,
                schoolDistance real,
                clinicDistance real,
                postOfficeDistance real,
                kindergartenDistance real,
                restaurantDistance real,
                collegeDistance real,
                pharmacyDistance real,
                ownership text,
                buildingMaterial text,
                condition text,
                hasParkingSpace boolean,
                hasBalcony boolean,
                hasElevator boolean,
                hasSecurity boolean,
                hasStorageRoom boolean,
                price real
            );
        """)

    conn.commit()
    conn.close()

def save_filenames_to_database():
    create_filenames_table_if_not_exists()
    
    conn = get_connection()
    
    cursor = conn.cursor()
    
    # Get a list of all CSV filenames in the directory
    path = '/mnt/d/Work/Portfolio/Apartment-Prices-in-Poland/data/raw_data/'
    csv_files = glob.glob(path + "apartments_*.csv")
    
    for csv_file in csv_files:
        # Check if the filename already exists in the database
        cursor.execute("SELECT COUNT(*) FROM filenames WHERE filename = %s", (csv_file,))
        count = cursor.fetchone()[0]
        
        if count == 0:
            # If the filename doesn't exist, insert it with flag = 1
            cursor.execute("INSERT INTO filenames (filename, flag) VALUES (%s, 1)", (csv_file,))
        else:
            # If the filename exists, update the flag to 1
            cursor.execute("UPDATE filenames SET flag = 1 WHERE filename = %s", (csv_file,))
    
    # Commit the changes and close the database connection
    conn.commit()
    conn.close()

def read_and_concat_data():
    create_raw_data_table_if_not_exists()
    
    # Connect to your PostgreSQL database
    conn = get_connection()
    
    # Get a list of filenames with flag = 1
    cursor = conn.cursor()
    cursor.execute("SELECT filename FROM filenames WHERE flag = 1")
    filenames = [record[0] for record in cursor.fetchall()]
    
    # Read and concatenate data from selected files
    dfs = []
    for filename in filenames:
        df = pd.read_csv(filename)
        dfs.append(df)
    
    # Concatenate dataframes
    concatenated_df = pd.concat(dfs, ignore_index=True)
    
    # Save concatenated data to PostgreSQL
    engine = create_engine(f'postgresql://{config("DB_USER")}:{config("DB_PASSWORD")}@{config("DB_HOST")}/{config("DB_NAME")}')
    concatenated_df.to_sql('raw_data', engine, if_exists='replace', index=False)
    
    # Update flag to 0 for processed files
    cursor.execute("UPDATE filenames SET flag = 0 WHERE filename IN %s", (tuple(filenames),))
    
    # Commit the changes and close the database connection
    conn.commit()
    conn.close()

with DAG('extract', default_args=default_args, schedule_interval='@monthly', catchup=False) as dag:
    
    del_files_before_run = BashOperator(
        task_id='delete_files_from_previous_run',
        bash_command='rm -rf /mnt/d/Work/Portfolio/Apartment-Prices-in-Poland/data/raw_data/* && rm -rf /mnt/d/Work/Portfolio/Apartment-Prices-in-Poland/data/zip/*'
    )
    extract = BashOperator(
        task_id='download_dataset',
        bash_command='kaggle datasets download -d krzysztofjamroz/apartment-prices-in-poland -p /mnt/d/Work/Portfolio/Apartment-Prices-in-Poland/data/zip'
    )
    
    unzip_data = BashOperator(
        task_id='unzip_dataset',
        bash_command='unzip /mnt/d/Work/Portfolio/Apartment-Prices-in-Poland/data/zip/apartment-prices-in-poland.zip -d /mnt/d/Work/Portfolio/Apartment-Prices-in-Poland/data/raw_data'
    )
    
    save_filenames = PythonOperator(
        task_id='save_filenames_to_database',
        python_callable=save_filenames_to_database
    )
    
    process_and_save = PythonOperator(
        task_id='read_and_concat_data',
        python_callable=read_and_concat_data
    )
    
    del_files_after_run = BashOperator(
        task_id='delete_files_after_run',
        bash_command='rm -rf /mnt/d/Work/Portfolio/Apartment-Prices-in-Poland/data/raw_data/* && rm -rf /mnt/d/Work/Portfolio/Apartment-Prices-in-Poland/data/zip/*'
    )
    
    del_files_before_run >> extract >> unzip_data >> save_filenames >> process_and_save >> del_files_after_run
