from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3,log_prints=True)
def extract_from_gcs(colour: str, year: int, month: int) -> Path:
    """
    Download trip data from GCS
    """
    gcs_path = f"data/{colour}/{colour}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("de-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"{gcs_path}")


@task(log_prints=True)
def transform_data(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df 


@task()
def write_to_bigquery(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("de-gcp-credentials")

    df.to_gbq(
        destination_table="trips_data_all.ny_trip_data",
        project_id="data-engineering-398114",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_biquery():
    """Main ETL flow to load data into Big Query"""
    colour = "yellow"
    year = 2021
    month = 1

    path = extract_from_gcs(colour, year, month)
    df = transform_data(path)
    write_to_bigquery(df)


if __name__ == "__main__":
    etl_gcs_to_biquery()