-- logging

{{ config(materialized='table') }}
{{ log("Starting yellow taxi transformation model", info=True) }}

-- YELLOW taxi transformations + simple error handling

WITH yellow_trip AS (
    SELECT
        stg_yellow_taxi.*,
        stg_emissions.co2_grams_per_mile,
        
        -- 1. calculate CO2 (handles nulls and negatives)
        CASE 
            WHEN stg_yellow_taxi.trip_distance IS NULL OR stg_emissions.co2_grams_per_mile IS NULL THEN NULL
            WHEN stg_yellow_taxi.trip_distance <= 0 OR stg_emissions.co2_grams_per_mile <= 0 THEN NULL
            ELSE (stg_yellow_taxi.trip_distance * stg_emissions.co2_grams_per_mile) / 1000.0 
        END AS trip_co2_kgs,

        -- 2. calculate average mph (prevents division by zero and invalid times)
        CASE 
            WHEN tpep_dropoff_datetime IS NULL OR tpep_pickup_datetime IS NULL THEN NULL
            WHEN tpep_dropoff_datetime <= tpep_pickup_datetime THEN NULL
            WHEN EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600.0 <= 0 THEN NULL
            ELSE stg_yellow_taxi.trip_distance / (EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600.0)
        END AS avg_mph,
        
        -- 3-6. extracting time fields (handles nulls)
        EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour_of_day,
        EXTRACT(DOW FROM tpep_pickup_datetime) AS day_of_week,
        EXTRACT(WEEK FROM tpep_pickup_datetime) AS week_of_year,
        EXTRACT(MONTH FROM tpep_pickup_datetime) AS month_of_year
        
    FROM {{ ref('stg_yellow_taxi') }}
    LEFT JOIN {{ ref('stg_emissions') }}
        ON stg_emissions.vehicle_type = 'yellow_taxi'
)

SELECT * FROM yellow_trip

{{ log("Finished yellow taxi transformation", info=True) }}