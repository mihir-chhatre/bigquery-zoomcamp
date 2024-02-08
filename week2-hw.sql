-- Setup:

-- Creating the external table
CREATE OR REPLACE EXTERNAL TABLE `dezoomcamp2024-mc9164.ny_taxi.green_tripdata_2022_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://zoomcamp2024/green_taxi_2022/*']
);

-- Creating table in BigQuery
CREATE OR REPLACE TABLE dezoomcamp2024-mc9164.ny_taxi.green_tripdata_2022_non_partitioned AS
SELECT * FROM dezoomcamp2024-mc9164.ny_taxi.green_tripdata_2022_external;

-- Question 1: What is count of records for the 2022 Green Taxi Data?
-- 840402 (directly available from storage info)

-- Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
SELECT COUNT(DISTINCT PULocationID) 
FROM `dezoomcamp2024-mc9164.ny_taxi.green_tripdata_2022_external`;

SELECT COUNT(DISTINCT PULocationID) 
FROM `dezoomcamp2024-mc9164.ny_taxi.green_tripdata_2022_non_partitioned`;

-- Question 3: How many records have a fare_amount of 0?
SELECT COUNT(*) as count_zero_fares
FROM `dezoomcamp2024-mc9164.ny_taxi.green_tripdata_2022_non_partitioned`
WHERE fare_amount=0;

-- Question 4: Partition by lpep_pickup_datetime Cluster on PUlocationID
CREATE TABLE `dezoomcamp2024-mc9164.ny_taxi.green_tripdata_2022_partitioned_clustered`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM `dezoomcamp2024-mc9164.ny_taxi.green_tripdata_2022_external`;

-- Question 5: Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
SELECT DISTINCT PULocationID
FROM `dezoomcamp2024-mc9164.ny_taxi.green_tripdata_2022_non_partitioned`
WHERE lpep_pickup_datetime >= '2022-06-01 00:00:00' 
AND lpep_pickup_datetime <= '2022-06-30 23:59:59';

SELECT DISTINCT PULocationID
FROM `dezoomcamp2024-mc9164.ny_taxi.green_tripdata_2022_partitioned_clustered`
WHERE lpep_pickup_datetime >= '2022-06-01 00:00:00' 
AND lpep_pickup_datetime <= '2022-06-30 23:59:59';

