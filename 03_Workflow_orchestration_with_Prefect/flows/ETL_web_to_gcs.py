import os
from pathlib2 import Path
import pandas as pd
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket

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
def etl_web_to_gcs():
    colour="yellow"
    year=2021
    month=1
    dataset_file=f"{colour}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{colour}/{dataset_file}.csv.gz"

    df=fetch_data(dataset_url)
    clean_df=clean_data(df)
    path = write_local(clean_df, colour, dataset_file)
    load_to_gcs(path)
    
if __name__=='__main__':
    etl_web_to_gcs()
    