import os
from pathlib2 import Path
import pandas as pd
from time import time
import datetime
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash

@task(retries=3,log_prints=True)
def fetch_data(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    tpep_pickup and tpep_dropoff are objects data 
    type and therefore we chage to datetime
    
    """
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """
    Write DataFrame out locally as parquet file
    
    """
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task()
def load_to_gcs(path: Path) -> None:
    """
    Upload local parquet file to GCS
    """
    gcs_block = GcsBucket.load("de-gcs")
    gcs_block.upload_from_path(from_path=f"{path}", to_path=path)
    return

@flow()
def etl_web_to_gcs(year:int,month:int,color:str):
    
    dataset_file=f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df=fetch_data(dataset_url)
    clean_df=clean_data(df)
    path = write_local(clean_df, color, dataset_file)
    load_to_gcs(path)

@flow()
def etl_parent_flow(
        months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    for month in months:
        etl_web_to_gcs(year, month, color)

    
if __name__=='__main__':
    color="yellow"
    months=[1,2,3]
    year= 2021
    etl_parent_flow(months,year,color)
    