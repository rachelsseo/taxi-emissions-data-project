import duckdb
import os
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

def load_parquet_files():
    con = None

    try:
        # connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        logger.info("Connected to DuckDB instance")

        # build yellow and green 2024 URLs

        yellow_urls = [
            f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-{month:02d}.parquet"
            for month in range(1, 13)
        ]
        green_urls = [
            f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-{month:02d}.parquet"
            for month in range(1, 13)
        ]

        # drop old tables if they exist

        con.execute("DROP TABLE IF EXISTS yellow_taxi_2024;")
        con.execute("DROP TABLE IF EXISTS green_taxi_2024;")
        con.execute("DROP TABLE IF EXISTS emissions;")

        # create new tables from parquet files
       
        con.execute(f"""
        CREATE TABLE yellow_taxi_2024 AS
        SELECT tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance
        FROM read_parquet([{', '.join([f"'{url}'" for url in yellow_urls])}]); 
        """)

        con.execute(f"""
        CREATE TABLE green_taxi_2024 AS
        SELECT lpep_pickup_datetime, lpep_dropoff_datetime, passenger_count, trip_distance
        FROM read_parquet([{', '.join([f"'{url}'" for url in green_urls])}]);
        """)


        # load emissions from CSV

        con.execute("""
            CREATE TABLE emissions AS
            SELECT * FROM read_csv_auto('data/vehicle_emissions.csv', header=True);
        """)

        # recording counts for verification

        yellow_count = con.execute("SELECT COUNT(*) FROM yellow_taxi_2024;").fetchone()[0]
        green_count = con.execute("SELECT COUNT(*) FROM green_taxi_2024;").fetchone()[0]
        emissions_count = con.execute("SELECT COUNT(*) FROM emissions;").fetchone()[0]

        print(f"Number of records for yellow taxi data before cleaning: {yellow_count}")
        print(f"Number of records for green taxi data before cleaning: {green_count}")
        print(f"Number of records for emissions data before cleaning: {emissions_count}")

        logger.info("Data load verification queries executed successfully")
        print("Parquet files loaded successfully into DuckDB.")

        return con

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files() 