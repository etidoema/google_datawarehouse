-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ultimate-aspect-410714.ny_taxi.external_green_tripdata`
OPTIONS (
      format = 'PARQUET',
      uris =  ['gs://mage_zoomcamp-etido-1/green_ny_taxi_data_2022/green_tripdata_2022-01.parquet',
               'gs://mage_zoomcamp-etido-1/green_ny_taxi_data_2022/green_tripdata_2022-02.parquet',
               'gs://mage_zoomcamp-etido-1/green_ny_taxi_data_2022/green_tripdata_2022-03.parquet',
               'gs://mage_zoomcamp-etido-1/green_ny_taxi_data_2022/green_tripdata_2022-04.parquet',
               'gs://mage_zoomcamp-etido-1/green_ny_taxi_data_2022/green_tripdata_2022-05 (1).parquet',
               'gs://mage_zoomcamp-etido-1/green_ny_taxi_data_2022/green_tripdata_2022-06.parquet',
               'gs://mage_zoomcamp-etido-1/green_ny_taxi_data_2022/green_tripdata_2022-07.parquet',
               'gs://mage_zoomcamp-etido-1/green_ny_taxi_data_2022/green_tripdata_2022-08.parquet',
               'gs://mage_zoomcamp-etido-1/green_ny_taxi_data_2022/green_tripdata_2022-09.parquet',
               'gs://mage_zoomcamp-etido-1/green_ny_taxi_data_2022/green_tripdata_2022-10.parquet',
               'gs://mage_zoomcamp-etido-1/green_ny_taxi_data_2022/green_tripdata_2022-11.parquet',
               'gs://mage_zoomcamp-etido-1/green_ny_taxi_data_2022/green_tripdata_2022-12.parquet'
              ]
);



-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE ultimate-aspect-410714.ny_taxi.green_tripdata_non_partitoned AS
SELECT * FROM ultimate-aspect-410714.ny_taxi.external_green_tripdata;


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE ultimate-aspect-410714.ny_taxi.green_tripdata_partitoned
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM ultimate-aspect-410714.ny_taxi.external_green_tripdata;


-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `ny_taxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'green_tripdata_partitoned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE ultimate-aspect-410714.ny_taxi.green_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM ultimate-aspect-410714.ny_taxi.external_green_tripdata;











--- QUESTION 1
-- Check 2022_green trip data
SELECT COUNT(*) FROM ultimate-aspect-410714.ny_taxi.external_green_tripdata;



--- QUESTION 2
--- Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
--- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

 SELECT COUNT(DISTINCT PULocationID) AS distinct_locationid
 FROM ultimate-aspect-410714.ny_taxi.external_green_tripdata;
-- 0B for Partitioned table

SELECT COUNT(DISTINCT PULocationID) AS distinct_locationid
 FROM ultimate-aspect-410714.ny_taxi.green_tripdata_non_partitoned;
-- 6.41mb for non partitioned table



-- Question 3
-- How many records have a fare_amount of 0?
 SELECT COUNT(*)
 FROM ultimate-aspect-410714.ny_taxi.green_tripdata_partitoned 
WHERE fare_amount = 0;



--- Question 4
-- What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime?
-- (Create a new table with this strategy)


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE ultimate-aspect-410714.ny_taxi.green_tripdata_partitoned_external
PARTITION BY DATE(lpep_pickup_datetime) AS
SELECT * FROM  ultimate-aspect-410714.ny_taxi.external_green_tripdata;


-- Creating a partition and cluster table
CREATE OR REPLACE TABLE ultimate-aspect-410714.ny_taxi.green_tripdata_partitoned_clusterted
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM ultimate-aspect-410714.ny_taxi.external_green_tripdata;


--- Question 5
-- Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
-- Use the materialized table you created earlier in your from clause and note the estimated bytes.
-- Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?

-- non partitioned table
SELECT COUNT(DISTINCT PULocationID) AS distinct_pulocationid
FROM ultimate-aspect-410714.ny_taxi.green_tripdata_non_partitoned
WHERE lpep_pickup_datetime BETWEEN TIMESTAMP('2022-06-01') AND TIMESTAMP('2022-06-30');


-- partitioned table
SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs
FROM ultimate-aspect-410714.ny_taxi.green_tripdata_partitoned
WHERE lpep_pickup_datetime BETWEEN TIMESTAMP('2022-06-01') AND TIMESTAMP('2022-06-30');




