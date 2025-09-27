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

        # cleaning yelow and green taxi data
        con.execute(f"""
        
            -- 1. create new tables & remove duplicates 
            CREATE TABLE clean_yellow_taxi AS SELECT DISTINCT * FROM yellow_taxi_2024;
            CREATE TABLE clean_green_taxi AS SELECT DISTINCT * FROM green_taxi_2024; 

            -- 2. remove trips with null/zero passengers
            DELETE FROM clean_yellow_taxi WHERE passenger_count IS NULL OR passenger_count = 0; 
            DELETE FROM clean_green_taxi WHERE passenger_count IS NULL OR passenger_count = 0; 

            -- 3. remove trips with zero distance and longer than 100 miles
            DELETE FROM clean_yellow_taxi WHERE trip_distance = 0 OR trip_distance > 100;
            DELETE FROM clean_green_taxi WHERE trip_distance = 0 OR trip_distance > 100;
            
            -- 4. remove trips lasting more than 1 day in length (86400 seconds) 
            DELETE FROM clean_yellow_taxi WHERE DATEDIFF('second', tpep_pickup_datetime, tpep_dropoff_datetime) > 86400;
            DELETE FROM clean_green_taxi WHERE DATEDIFF('second', lpep_pickup_datetime, lpep_dropoff_datetime) > 86400;
            
            -- 5. drop old tables and rename cleaned tables
            DROP TABLE yellow_taxi_2024;
            DROP TABLE green_taxi_2024;
            ALTER TABLE clean_yellow_taxi RENAME TO yellow_taxi_2024;
            ALTER TABLE clean_green_taxi RENAME TO green_taxi_2024;
        """)
        
        # validation checks 

        # minimum passengers -> should not return 0

        yellow_minpassengers = con.execute("SELECT MIN(passenger_count) FROM yellow_taxi_2024").fetchone()
        green_minpassengers = con.execute("SELECT MIN(passenger_count) FROM green_taxi_2024").fetchone()
        print(f"Min passengers (yellow): {yellow_minpassengers[0]}")
        print(f"Min passengers (green):  {green_minpassengers[0]}")

        # max/min distance -> should be between 0 and 100
        yellow_maxdistance = con.execute("SELECT MAX(trip_distance) FROM yellow_taxi_2024").fetchone()
        green_maxdistance = con.execute("SELECT MAX(trip_distance) FROM green_taxi_2024").fetchone()
        print(f"Max distance (yellow): {yellow_maxdistance[0]}")
        print(f"Max distance (green):  {green_maxdistance[0]}")

        yellow_mindistance = con.execute("SELECT MIN(trip_distance) FROM yellow_taxi_2024").fetchone()
        green_mindistance = con.execute("SELECT MIN(trip_distance) FROM green_taxi_2024").fetchone()
        print(f"Min distance (yellow): {yellow_mindistance[0]}")
        print(f"Min distance (green):  {green_mindistance[0]}")


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
        
        print("Cleaned and validated tables for both yellow and green taxi data.")

        logger.info("Cleaned and validated tables for both yellow and green taxi data.")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    clean_parquet()