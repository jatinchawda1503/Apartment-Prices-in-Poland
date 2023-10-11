from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.decorators import task

from datetime import datetime, timedelta
import pandas as pd
import numpy as np
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

def fill_missing_distance(df, column):
    df[column] = df[column].transform(lambda x: 10 if pd.isna(x) else x)
    return df

def fill_missing_type(df, column):
    df[column] = df.apply(lambda x: 'tenement' if (pd.isna(x['type']) and  x['buildYear'] < 1960) else x['type'], axis=1)
    df[column] = df.apply(lambda x: 'blockOfFlats' if (pd.isna(x['type']) and  x['buildYear'] < 2000) else x['type'], axis=1)
    df[column] = df.apply(lambda x: 'blockOfFlats' if (pd.isna(x['type']) and  x['ownership'] == 'cooperative') else x['type'], axis=1)
    df[column] = df.apply(lambda x: ('blockOfFlats' if(np.random.randint(0, 2) == 0) else 'apartmentBuilding') if (pd.isna(x['type'])) else x['type'], axis=1)
    return df

def fill_missing_material(df, column):
    df[column] = df.apply(lambda x: 'concreteSlab' if (pd.isna(x['buildingMaterial']) and x['buildYear'] >= 1960 and x['buildYear'] <= 1980) else x['buildingMaterial'], axis=1)
    df[column] = df.apply(lambda x: 'brick' if pd.isna(x['buildingMaterial']) else x['buildingMaterial'], axis=1)
    return df

def fill_missing_floorCount(df, column):
    df[column] = df.apply(lambda x: max(x['floor'], 4) if pd.isna(x['floorCount']) else x['floorCount'], axis=1)
    df[column] = df.apply(lambda x: 4 if pd.isna(x['floorCount']) else x['floorCount'], axis=1)
    return df

def fill_missing_floor(df, column):
    df[column] = df.apply(lambda x: np.random.randint(1, x['floorCount']+1) if pd.isna(x['floor']) else x['floor'], axis=1)
    return df

def fill_missing_elevator(df, column):
    df[column] = df.apply(lambda x: 'no' if (pd.isna(x['hasElevator']) and x['floorCount'] <= 4) else x['hasElevator'], axis=1)
    df[column] = df.apply(lambda x: 'yes' if (pd.isna(x['hasElevator']) and x['floorCount'] > 4) else x['hasElevator'], axis=1)
    return df

def fill_missing_buildYear(df, column):
    df[column] = df.apply(lambda x: round(np.random.normal(loc=1925, scale=25)) if (pd.isna(x['buildYear']) and  x['type'] == 'tenement') else x['buildYear'], axis=1)
    df[column] = df.apply(lambda x: (2023 - round(abs(np.random.normal(loc=0, scale=15)))) if (pd.isna(x['buildYear']) and  x['type'] == 'apartmentBuilding') else x['buildYear'], axis=1)
    df[column] = df.apply(lambda x: round(np.random.normal(loc=1970, scale=15)) if (pd.isna(x['buildYear']) and  x['type'] == 'blockOfFlats') else x['buildYear'], axis=1)
    return df

def create_raw_data_table_if_not_exists():
    conn = get_connection()
    cursor = conn.cursor()
    
    # Check if the raw_data table exists
    cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'cleaned_data')")
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        # If the table doesn't exist, create it
        cursor.execute("""
            CREATE TABLE cleaned_data (
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

with DAG('preprocess', default_args=default_args, schedule_interval='@monthly', catchup=False) as dag:
    
    # read the data 
    @task()
    def read_data():
        conn = get_connection()
        df = pd.read_sql_query('select * from "raw_data"', conn)
        conn.close()
        return df
    
    @task()
    def preprocess_data(df):
        df = df.drop_duplicates().reset_index(drop=True)
        for cols in ['collegeDistance', 'clinicDistance', 'restaurantDistance', 'pharmacyDistance', 'postOfficeDistance', 'kindergartenDistance', 'schoolDistance']:
            df = fill_missing_distance(df, cols)
        df = fill_missing_type(df, 'type')
        df = fill_missing_material(df, 'buildingMaterial')
        df = fill_missing_floorCount(df, 'floorCount')
        df = fill_missing_floor(df, 'floor')
        df = fill_missing_elevator(df, 'hasElevator')
        df = fill_missing_buildYear(df, 'buildYear')
        df = df.drop(columns=['condition'])
        return df
    
    @task()
    def save_data(df):
        create_raw_data_table_if_not_exists()
        engine = create_engine(f'postgresql://{config("DB_USER")}:{config("DB_PASSWORD")}@{config("DB_HOST")}/{config("DB_NAME")}')
        df.to_sql('cleaned_data', engine, if_exists='replace', index=False)
        
        
    read_data = read_data()
    preprocess_data = preprocess_data(read_data)
    save_data = save_data(preprocess_data)
    
    read_data >> preprocess_data >> save_data
    

        