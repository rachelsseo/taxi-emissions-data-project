-- Logging

{{ config(materialized='table') }}
{{ log("Starting green taxi transformation model", info=True) }}


-- GREEN taxi transformations

WITH green_trip AS (
    SELECT
        stg_green_taxi.*,
        stg_emissions.co2_grams_per_mile,
        (stg_green_taxi.trip_distance * stg_emissions.co2_grams_per_mile) / 1000.0 AS trip_co2_kgs,

-- 2. Calculate average miles per hour
    CASE 
    WHEN EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) / 3600.0 > 0
    THEN stg_green_taxi.trip_distance / (EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) / 3600.0)
    ELSE 0 
    END AS avg_mph,
        
-- 3. Extract hour of day from pickup time
    EXTRACT(HOUR FROM lpep_pickup_datetime) AS hour_of_day,
        
-- 4. Extract day of week from pickup time (1=Sunday, 7=Saturday)
    EXTRACT(DOW FROM lpep_pickup_datetime) AS day_of_week,
        
-- 5. Extract week number from pickup time
    EXTRACT(WEEK FROM lpep_pickup_datetime) AS week_of_year,
        
-- 6. Extract month from pickup time
    EXTRACT(MONTH FROM lpep_pickup_datetime) AS month_of_year
        
    FROM {{ ref('stg_green_taxi') }}
    LEFT JOIN {{ ref('stg_emissions') }}
        ON stg_emissions.vehicle_type = 'green_taxi'
)

{{ log("Finished green taxi transformation model", info=True) }}

SELECT * FROM green_trip