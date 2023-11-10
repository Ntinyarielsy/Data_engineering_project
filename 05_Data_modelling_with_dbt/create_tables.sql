CREATE OR REPLACE EXTERNAL TABLE data-engineering-398114.ny_taxi_data.fhv_tripdata
OPTIONS (
  format = 'parquet',
  uris = ['gs://de_data_lake_data-engineering-398114/fhv/*.parquet']
  );

CREATE OR REPLACE EXTERNAL TABLE data-engineering-398114.ny_taxi_data.green_tripdata
OPTIONS (
  format = 'parquet',
  uris = ['gs://de_data_lake_data-engineering-398114/green/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE data-engineering-398114.ny_taxi_data.yellow_tripdata
OPTIONS (
  format = 'parquet',
  uris = ['gs://de_data_lake_data-engineering-398114/yellow/*.parquet']
);

