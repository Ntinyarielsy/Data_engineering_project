import os
import argparse
import datetime
from time import time
import pandas as pd
from sqlalchemy import create_engine, inspect
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True,retries=3,cache_key_fn=task_input_hash,cache_expiration=datetime.timedelta(days=1))
def extract_data(url):
    # Determine the output file name based on the URL
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'
    os.system(f"wget {url} -O {csv_name}")
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    return df

@task(log_prints=True)
def data_transform(df):
        print(f"missing data in passenger count: {df['passenger_count'].isin([0]).sum()}")
        df=df[df['passenger_count'] !=0]
        print(f"missing data in passenger count: {df['passenger_count'].isin([0]).sum()}")
        return df
       
@task(log_prints=True,retries=3)
def load_data(table_name,df):
    connection_block=SqlAlchemyConnector.load("postgres-connection")
    with connection_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')

@flow(name='data_ingest_subflow') 
def log_subflow(table_name:str):
    print("logging subflow for: {table_name}")
    
               
@flow(name="data-ingest-flow")
def main(table_name: str):
    url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    log_subflow(table_name)
    
    raw_data=extract_data(url)
    data=data_transform(raw_data)
    load_data(table_name,data)

if __name__ == '__main__':
    main("yellow_taxi_data")
    

    