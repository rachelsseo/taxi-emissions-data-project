import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)

def clean_parquet():

    con = None

    try:
        # connect to local DuckDB
        con = duckdb.connect(database='emissions.duckdb', read_only=False)

        # cleaning yellow and green taxi data
        con.execute(f"""
        -- 1. clean yellow_taxi data
        CREATE TABLE clean_yellow_taxi AS 
        SELECT DISTINCT *
        FROM yellow_taxi_2024
        WHERE passenger_count IS NOT NULL 
        AND passenger_count > 0
        AND trip_distance > 0 
        AND trip_distance <= 100
        AND DATEDIFF('second', tpep_pickup_datetime, tpep_dropoff_datetime) <= 86400;

        -- 2. clean green_taxi data
        CREATE TABLE clean_green_taxi AS 
        SELECT DISTINCT *
        FROM green_taxi_2024
        WHERE passenger_count IS NOT NULL 
        AND passenger_count > 0
        AND trip_distance > 0 
        AND trip_distance <= 100
        AND DATEDIFF('second', lpep_pickup_datetime, lpep_dropoff_datetime) <= 86400;
    
        -- 3. Drop old tables and rename
        DROP TABLE yellow_taxi_2024;
        DROP TABLE green_taxi_2024;
        ALTER TABLE clean_yellow_taxi RENAME TO yellow_taxi_2024;
        ALTER TABLE clean_green_taxi RENAME TO green_taxi_2024;
        """)
        
        # validation checks 

        # duplicates -> should be 0
        yellow_duplicates = con.execute("""
        SELECT COUNT(*) - (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM yellow_taxi_2024))
        FROM yellow_taxi_2024;
        """).fetchone()

        green_duplicates = con.execute("""
            SELECT COUNT (*) - (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM green_taxi_2024))
            FROM green_taxi_2024;
        """).fetchone()

        print(f"Yellow taxi duplicates: {yellow_duplicates[0]}")
        print(f"Green taxi duplicates:  {green_duplicates[0]}")

        # logging duplicates
        logger.info(f"Yellow taxi duplicates: {yellow_duplicates[0]}")
        logger.info(f"Green taxi duplicates:  {green_duplicates[0]}")

        # minimum passengers -> should not return 0

        yellow_minpassengers = con.execute("SELECT MIN(passenger_count) FROM yellow_taxi_2024").fetchone()
        green_minpassengers = con.execute("SELECT MIN(passenger_count) FROM green_taxi_2024").fetchone()
        print(f"Min passengers (yellow): {yellow_minpassengers[0]}")
        print(f"Min passengers (green):  {green_minpassengers[0]}")

        # logging minimum passengers
        logger.info(f"Min passengers (yellow): {yellow_minpassengers[0]}")
        logger.info(f"Min passengers (green):  {green_minpassengers[0]}")

        # max/min distance -> should be between 0 and 100
        yellow_maxdistance = con.execute("SELECT MAX(trip_distance) FROM yellow_taxi_2024").fetchone()
        green_maxdistance = con.execute("SELECT MAX(trip_distance) FROM green_taxi_2024").fetchone()
        print(f"Max distance (yellow): {yellow_maxdistance[0]}")
        print(f"Max distance (green):  {green_maxdistance[0]}")

        # logging max distance
        logger.info(f"Max distance (yellow): {yellow_maxdistance[0]}")
        logger.info(f"Max distance (green):  {green_maxdistance[0]}")

        yellow_mindistance = con.execute("SELECT MIN(trip_distance) FROM yellow_taxi_2024").fetchone()
        green_mindistance = con.execute("SELECT MIN(trip_distance) FROM green_taxi_2024").fetchone()
        print(f"Min distance (yellow): {yellow_mindistance[0]}")
        print(f"Min distance (green):  {green_mindistance[0]}")

        # logging min distance
        logger.info(f"Min distance (yellow): {yellow_mindistance[0]}")
        logger.info(f"Min distance (green):  {green_mindistance[0]}")


        # max trip duration -> should be less than 86400 seconds (1 day)
        yellow_maxsecs = con.execute("""
            SELECT MAX(DATEDIFF('second', tpep_pickup_datetime, tpep_dropoff_datetime))
            FROM yellow_taxi_2024;
        """).fetchone()

        green_maxsecs = con.execute("""
            SELECT MAX(DATEDIFF('second', lpep_pickup_datetime, lpep_dropoff_datetime))
            FROM green_taxi_2024;
        """).fetchone()

        print(f"Max trip duration (yellow) in seconds: {yellow_maxsecs[0]}")
        print(f"Max trip duration (green) in seconds:  {green_maxsecs[0]}")

        # logging max trip duration
        logger.info(f"Max trip duration (yellow) in seconds: {yellow_maxsecs[0]}")
        logger.info(f"Max trip duration (green) in seconds:  {green_maxsecs[0]}")
        
        print("Cleaned and validated tables for both yellow and green taxi data.")

        logger.info("Cleaned and validated tables for both yellow and green taxi data.")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    clean_parquet()