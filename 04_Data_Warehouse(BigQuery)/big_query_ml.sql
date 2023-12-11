-- SELECT THE COLUMNS INTERESTED FOR YOU
SELECT passenger_count, trip_distance, PULocationID, DOLocationID, payment_type, fare_amount, tolls_amount, tip_amount
FROM `data-engineering-398114.trips_data_all.rides_partitoned` 
WHERE fare_amount != 0;

-- CREATE A ML TABLE WITH APPROPRIATE TYPE
CREATE OR REPLACE TABLE `data-engineering-398114.trips_data_all.rides_ml` (
`passenger_count` INTEGER,
`trip_distance` FLOAT64,
`PULocationID` STRING,
`DOLocationID` STRING,
`payment_type` STRING,
`fare_amount` FLOAT64,
`tolls_amount` FLOAT64,
`tip_amount` FLOAT64) AS 
(SELECT cast(passenger_count AS INTEGER), trip_distance, cast(PULocationID AS STRING), CAST(DOLocationID AS STRING),
  CAST(payment_type AS STRING), fare_amount, tolls_amount, tip_amount
  FROM `data-engineering-398114.trips_data_all.rides_partitoned` 
  WHERE fare_amount != 0
);

--create a linear regression model to predict tip_amount
CREATE OR REPLACE MODEL `data-engineering-398114.trips_data_all.tip_amount_model`
OPTIONS
  (model_type='linear_reg',
  input_label_cols=['tip_amount'],
  DATA_SPLIT_METHOD='AUTO_SPLIT') AS
SELECT *
FROM `data-engineering-398114.trips_data_all.rides_ml`
WHERE tip_amount IS NOT NULL;

-- CHECK FEATURES
SELECT * 
FROM ML.FEATURE_INFO(MODEL `data-engineering-398114.trips_data_all.tip_amount_model`);

--evaluating our model
SELECT *
FROM ML.EVALUATE(MODEL `data-engineering-398114.trips_data_all.tip_amount_model`,
    (SELECT * 
    FROM `data-engineering-398114.trips_data_all.rides_ml` 
     WHERE tip_amount IS NOT NULL));

--prediction
SELECT *
FROM
  ML.PREDICT(MODEL `data-engineering-398114.trips_data_all.tip_amount_model`,
    (SELECT *
    FROM `data-engineering-398114.trips_data_all.rides_ml`
    WHERE tip_amount IS NOT NULL));

-- PREDICT AND EXPLAIN
SELECT *
FROM
ML.EXPLAIN_PREDICT(MODEL `data-engineering-398114.trips_data_all.tip_amount_model`,
   (SELECT *
    FROM `data-engineering-398114.trips_data_all.rides_ml`
    WHERE tip_amount IS NOT NULL), 
STRUCT(3 as top_k_features));

--hyper parameter tuning
CREATE OR REPLACE MODEL `data-engineering-398114.trips_data_all.tip_amount_hypertuned_model`
OPTIONS
(model_type='linear_reg',
input_label_cols=['tip_amount'],
DATA_SPLIT_METHOD='AUTO_SPLIT',
num_trials=5,
max_parallel_trials=2,
l1_reg=hparam_range(0, 20),
l2_reg=hparam_candidates([0, 0.1, 1, 10])) AS
SELECT *
FROM `data-engineering-398114.trips_data_all.rides_ml`
WHERE tip_amount IS NOT NULL;
