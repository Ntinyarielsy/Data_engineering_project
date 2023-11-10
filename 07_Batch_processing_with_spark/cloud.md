## Instructions to Run spark in the cloud


## Local cluster and spark submit

Starting a stand-alone cluster
```
spark-class org.apache.spark.deploy.master.Master

```
Staring a worker
```
spark-class org.apache.spark.deploy.worker.Worker spark://172.24.160.1:7077

```
Running a python script
```
python3 code/spark_cluster.py \
    --input_green "C:/data_engineering_project/07_Batch_processing_with_spark/data/pq/green/2020/*/" \
    --input_yellow "C:/data_engineering_project/07_Batch_processing_with_spark/data/pq/yellow/2020/*/" \
    --output "C:/data_engineering_project/07_Batch_processing_with_spark/data/report/revenue/revenue-2019"
```
Spark submit
```
spark-submit \
   --master="spark://172.24.160.1:7077" \
   code/spark_cluster.py \
    --input_green "C:/data_engineering_project/07_Batch_processing_with_spark/data/pq/green/2019/*/" \
    --input_yellow "C:/data_engineering_project/07_Batch_processing_with_spark/data/pq/yellow/2019/*/" \
    --output "C:/data_engineering_project/07_Batch_processing_with_spark/data/report/revenue/revenue-2019"

```

### submitting a job via google web ui
```
## Path to the script location in gsc
gs://de_data_lake_data-engineering-398114/pq/code/spark_cluster.py

##arguments
    --input_green=gs://de_data_lake_data-engineering-398114/pq/green/2019/*/ \
    --input_yellow=gs://de_data_lake_data-engineering-398114/pq/yellow/2019/*/ \
    --output=gs://de_data_lake_data-engineering-398114/pq/report
```
### submitting a job via gcloud SDK
```
gcloud dataproc jobs submit pyspark ^
--cluster=datatalk-de-project-cluster ^
--region=europe-west6 ^
gs://de_data_lake_data-engineering-398114/pq/code/spark_cluster.py ^
-- ^
--input_green=gs://de_data_lake_data-engineering-398114/pq/green/2020/* ^
--input_yellow=gs://de_data_lake_data-engineering-398114/pq/yellow/2020/* ^
--output=gs://de_data_lake_data-engineering-398114/pq/report-2020
```


## connecting spark to bigquery
```
gcloud dataproc jobs submit pyspark ^
--cluster=datatalk-de-project-cluster ^
--region=europe-west6 ^
gs://de_data_lake_data-engineering-398114/pq/code/spark_cluster_to_bigquery.py ^
-- ^
--input_green=gs://de_data_lake_data-engineering-398114/pq/green/2020/* ^
--input_yellow=gs://de_data_lake_data-engineering-398114/pq/yellow/2020/* ^
--output=ny_taxi_data.reports-2020
```