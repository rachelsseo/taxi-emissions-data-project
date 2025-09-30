-- logging

{{ config(materialized='table') }}
{{ log("Starting green taxi transformation model", info=True) }}


-- GREEN taxi transformations + simple error handling

WITH green_trip AS (
    SELECT
        stg_green_taxi.*,
        stg_emissions.co2_grams_per_mile,
        
        -- 1. calculate CO2 (handles nulls and negatives)
        CASE 
            WHEN stg_green_taxi.trip_distance IS NULL OR stg_emissions.co2_grams_per_mile IS NULL THEN NULL
            WHEN stg_green_taxi.trip_distance <= 0 OR stg_emissions.co2_grams_per_mile <= 0 THEN NULL
            ELSE (stg_green_taxi.trip_distance * stg_emissions.co2_grams_per_mile) / 1000.0 
        END AS trip_co2_kgs,

        -- 2. calculate average mph (prevents division by zero and invalid times)
        CASE 
            WHEN lpep_dropoff_datetime IS NULL OR lpep_pickup_datetime IS NULL THEN NULL
            WHEN lpep_dropoff_datetime <= lpep_pickup_datetime THEN NULL
            WHEN EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) / 3600.0 <= 0 THEN NULL
            ELSE stg_green_taxi.trip_distance / (EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) / 3600.0)
        END AS avg_mph,
        
        -- 3-6. extracting time fields (handles nulls)
        EXTRACT(HOUR FROM lpep_pickup_datetime) AS hour_of_day,
        EXTRACT(DOW FROM lpep_pickup_datetime) AS day_of_week,
        EXTRACT(WEEK FROM lpep_pickup_datetime) AS week_of_year,
        EXTRACT(MONTH FROM lpep_pickup_datetime) AS month_of_year
        
    FROM {{ ref('stg_green_taxi') }}
    LEFT JOIN {{ ref('stg_emissions') }}
        ON stg_emissions.vehicle_type = 'green_taxi'
)

SELECT * FROM green_trip
{{ log("Completed green taxi transformation model", info=True) }}