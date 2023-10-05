from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.decorators import task
 
from datetime import datetime, timedelta
import pandas as pd
import glob
import zipfile

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 5),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}


def read_and_concat():
    path = r'/mnt/d/Work/Portfolio/Apartment-Prices-in-Poland/data/raw_data'
    all_files = glob.glob(path + "apartment_*.csv")
    li = []
    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)
    df = pd.concat(li, axis=0, ignore_index=True)
    df.to_csv('/mnt/d/Work/Portfolio/Apartment-Prices-in-Poland/data/processed_data/apartment_prices.csv', index=False)
    

with DAG('extract', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    extract = BashOperator(
        task_id='download_dataset',
        bash_command='kaggle datasets download -d krzysztofjamroz/apartment-prices-in-poland -p /mnt/d/Work/Portfolio/Apartment-Prices-in-Poland/data/zip'
    )
    
    unzip_data = BashOperator(
        task_id='unzip_dataset',
        bash_command='unzip /mnt/d/Work/Portfolio/Apartment-Prices-in-Poland/data/zip/apartment-prices-in-poland.zip -d /mnt/d/Work/Portfolio/Apartment-Prices-in-Poland/data/raw_data'
    )
    
    read_and_concat = PythonOperator(
        task_id='read_and_concat',
        python_callable=read_and_concat
    )
    

    extract >> unzip_data >> read_and_concat
    
    
    
    
    

    
