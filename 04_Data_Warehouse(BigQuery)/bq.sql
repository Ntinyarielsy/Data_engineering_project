-- Query public available table
SELECT station_id, name FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;


-- Creating external table referring to gcs path 
-- rides table contains two months data January and February
CREATE OR REPLACE EXTERNAL TABLE `data-engineering-398114.trips_data_all.rides`
OPTIONS (
  format = 'parquet',
  uris = ['gs://de_data_lake_data-engineering-398114/data/yellow/yellow_tripdata_2021-02.parquet','gs://de_data_lake_data-engineering-398114/data/yellow/yellow_tripdata_2021-01.parquet']
);

-- Check rides trip data
SELECT * FROM data-engineering-398114.trips_data_all.rides;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE data-engineering-398114.trips_data_all.rides_non_partitoned AS
SELECT * FROM data-engineering-398114.trips_data_all.rides;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE data-engineering-398114.trips_data_all.rides_partitoned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM data-engineering-398114.trips_data_all.rides;

-- Impact of partition
-- Scanning 40.33 MB of data 
SELECT DISTINCT(VendorID)
FROM data-engineering-398114.trips_data_all.rides_non_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-02-01' AND '2021-02-27';

-- Scanning ~19.62 MB of DATA
SELECT DISTINCT(VendorID)
FROM data-engineering-398114.trips_data_all.rides_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-02-01' AND '2021-02-27';

-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `trips_data_all.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'rides_partitoned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE data-engineering-398114.trips_data_all.rides_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM data-engineering-398114.trips_data_all.rides;

-- Query scans  GB
SELECT count(*) as trips
FROM taxi-rides-ny.nytaxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

-- Query scans MB
SELECT count(*) as trips
FROM taxi-rides-ny.nytaxi.yellow_tripdata_partitoned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;