import duckdb
import logging
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analysis.log'
    )
logger = logging.getLogger(__name__)


def analysis_parquet():

    con = None

    try:
        # connect to local DuckDB
        con = duckdb.connect('emissions.duckdb', read_only=False)

        # combined view for analysis
        con.execute("DROP VIEW IF EXISTS all_taxi")

        con.execute("""
            CREATE OR REPLACE VIEW all_taxi AS
            SELECT *, 'Yellow' AS cab_type FROM yellow_trip
            UNION ALL
            SELECT *, 'Green' AS cab_type FROM green_trip
        """)

        print("=== 2024 NYC TAXI CO2 EMISSIONS ANALYSIS REPORT ===\n")

        # 1. largest carbon producing trip for each cab type
        print("1. Single largest carbon producing trip:")

        largest_trips = con.execute("""
        SELECT 
            cab_type,
            MAX(trip_co2_kgs) as max_co2_kgs,
            ANY_VALUE(trip_distance) as trip_distance
        FROM all_taxi 
        WHERE trip_co2_kgs IS NOT NULL
        GROUP BY cab_type
        ORDER BY max_co2_kgs DESC
        """).fetchdf()

        # for loop to print results based on taxi type
        for _, row in largest_trips.iterrows():
            print(f"{row['cab_type']} Taxi: {row['max_co2_kgs']:.3f} kg CO2")
            print(f"   Distance: {row['trip_distance']:.2f} miles")
            print()

        # logging carbon heavy trip values
        logger.info(f"Largest Yellow Taxi Trip CO2: {largest_trips.iloc[0]['max_co2_kgs']:.3f} kg")
        logger.info(f"Largest Green Taxi Trip CO2: {largest_trips.iloc[1]['max_co2_kgs']:.3f} kg")
        

        # 2. most/least carbon heavy hours of the day
        print("2. Carbon heavy/light hours of the day:")

        hourly_analysis = con.execute("""
            SELECT 
                cab_type,
                hour_of_day,
                AVG(trip_co2_kgs) as avg_co2_kgs,
                COUNT(*) as trip_count
            FROM all_taxi 
            WHERE trip_co2_kgs IS NOT NULL
            GROUP BY cab_type, hour_of_day
            ORDER BY cab_type, avg_co2_kgs DESC
        """).fetchdf()

        for cab_type in ['Yellow', 'Green']:
            cab_data = hourly_analysis[hourly_analysis['cab_type'] == cab_type]
            heaviest_hour = cab_data.iloc[0]
            lightest_hour = cab_data.iloc[-1]
    
            print(f"{cab_type} Taxi:")
            print(f"   Carbon Heaviest Hour: {heaviest_hour['hour_of_day']}:00 ({heaviest_hour['avg_co2_kgs']:.4f} kg avg)")
            print(f"   Carbon Lightest Hour: {lightest_hour['hour_of_day']}:00 ({lightest_hour['avg_co2_kgs']:.4f} kg avg)")
            print()

        # logging carbon heavy/light hours
        logger.info(f"{cab_type} Taxi Heaviest Hour: {heaviest_hour['hour_of_day']}:00 ({heaviest_hour['avg_co2_kgs']:.4f} kg avg)")
        logger.info(f"{cab_type} Taxi Lightest Hour: {lightest_hour['hour_of_day']}:00 ({lightest_hour['avg_co2_kgs']:.4f} kg avg)")

        # 3. most/least carbon heavy days of the week
        print("3. Carbon heavy/light days of the week:")

        day_names = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']

        daily_analysis = con.execute("""
            SELECT 
                cab_type,
                day_of_week,
                AVG(trip_co2_kgs) as avg_co2_kgs,
                COUNT(*) as trip_count
            FROM all_taxi 
            WHERE trip_co2_kgs IS NOT NULL
            GROUP BY cab_type, day_of_week
            ORDER BY cab_type, avg_co2_kgs DESC
        """).fetchdf()

        for cab_type in ['Yellow', 'Green']:
            cab_data = daily_analysis[daily_analysis['cab_type'] == cab_type]
            heaviest_day = cab_data.iloc[0]
            lightest_day = cab_data.iloc[-1]
    
            print(f"{cab_type} Taxi:")
            print(f"   Carbon Heaviest Day: {day_names[int(heaviest_day['day_of_week'])]} ({heaviest_day['avg_co2_kgs']:.4f} kg avg)")
            print(f"   Carbon Lightest Day: {day_names[int(lightest_day['day_of_week'])]} ({lightest_day['avg_co2_kgs']:.4f} kg avg)")
            print()
        
        # logging carbon heavy/light days
        logger.info(f"{cab_type} Taxi Heaviest Day: {day_names[int(heaviest_day['day_of_week'])]} ({heaviest_day['avg_co2_kgs']:.4f} kg avg)")
        logger.info(f"{cab_type} Taxi Lightest Day: {day_names[int(lightest_day['day_of_week'])]} ({lightest_day['avg_co2_kgs']:.4f} kg avg)")


        # 4. most/least carbon heavy weeks of the year
        print("4. Carbon heavy/light weeks of the year:")

        weekly_analysis = con.execute("""
            SELECT 
                cab_type,
                week_of_year,
                AVG(trip_co2_kgs) as avg_co2_kgs,
                COUNT(*) as trip_count
            FROM all_taxi 
            WHERE trip_co2_kgs IS NOT NULL AND week_of_year IS NOT NULL
            GROUP BY cab_type, week_of_year
            ORDER BY cab_type, avg_co2_kgs DESC
        """).fetchdf()

        for cab_type in ['Yellow', 'Green']:
            cab_data = weekly_analysis[weekly_analysis['cab_type'] == cab_type]
            heaviest_week = cab_data.iloc[0]
            lightest_week = cab_data.iloc[-1]
    
            print(f"{cab_type} Taxi:")
            print(f"   Carbon Heaviest Week: Week {heaviest_week['week_of_year']} ({heaviest_week['avg_co2_kgs']:.4f} kg avg)")
            print(f"   Carbon Lightest Week: Week {lightest_week['week_of_year']} ({lightest_week['avg_co2_kgs']:.4f} kg avg)")
            print()

        # logging carbon heavy/light weeks
        logger.info(f"{cab_type} Taxi Heaviest Week: Week {heaviest_week['week_of_year']} ({heaviest_week['avg_co2_kgs']:.4f} kg avg)")
        logger.info(f"{cab_type} Taxi Lightest Week: Week {lightest_week['week_of_year']} ({lightest_week['avg_co2_kgs']:.4f} kg avg)")

        # 5. most/least carbon heavy months of the year
        print("5. Carbon heavy/light months of the year:")

        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
               'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

        monthly_analysis = con.execute("""
            SELECT 
                cab_type,
                month_of_year,
                AVG(trip_co2_kgs) as avg_co2_kgs,
                SUM(trip_co2_kgs) as total_co2_kgs,
                COUNT(*) as trip_count
            FROM all_taxi 
            WHERE trip_co2_kgs IS NOT NULL
            GROUP BY cab_type, month_of_year
            ORDER BY cab_type, avg_co2_kgs DESC
        """).fetchdf()

        for cab_type in ['Yellow', 'Green']:
            cab_data = monthly_analysis[monthly_analysis['cab_type'] == cab_type]
            heaviest_month = cab_data.iloc[0]
            lightest_month = cab_data.iloc[-1]
    
            print(f"{cab_type} Taxi:")
            print(f"   Carbon Heaviest Month: {month_names[int(heaviest_month['month_of_year'])-1]} ({heaviest_month['avg_co2_kgs']:.4f} kg avg)")
            print(f"   Carbon Lightest Month: {month_names[int(lightest_month['month_of_year'])-1]} ({lightest_month['avg_co2_kgs']:.4f} kg avg)")
            print()

        # logging carbon heavy/light months
        logger.info(f"{cab_type} Taxi Heaviest Month: {month_names[int(heaviest_month['month_of_year'])-1]} ({heaviest_month['avg_co2_kgs']:.4f} kg avg)")
        logger.info(f"{cab_type} Taxi Lightest Month: {month_names[int(lightest_month['month_of_year'])-1]} ({lightest_month['avg_co2_kgs']:.4f} kg avg)")

        logger.info("Completed analysis of taxi CO2 emissions.")

        # 6. generate time-series plot
        print("6. Generating Co2 emissions visualization...")

        # data for plotting
        plot_data = monthly_analysis.copy()
        plot_data['month_name'] = plot_data['month_of_year'].apply(lambda x: month_names[int(x)-1])

        # subplot for better layout
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 12))

        # yellow taxi CO2 by month (bar chart)
        yellow_data = plot_data[plot_data['cab_type'] == 'Yellow'].sort_values('month_of_year')
        ax1.bar(yellow_data['month_name'], yellow_data['total_co2_kgs'], color='gold', alpha=0.8)

        ax1.set_xlabel('Month')
        ax1.set_ylabel('Total CO2 Emissions (kg)')
        ax1.set_title('Monthly CO2 Emissions - Yellow Taxi')
        ax1.set_xticks(range(len(month_names)))
        ax1.set_xticklabels(month_names, rotation=45)
        ax1.grid(True, alpha=0.3)

        # green taxi CO2 by month (bar chart)
        green_data = plot_data[plot_data['cab_type'] == 'Green'].sort_values('month_of_year')
        ax2.bar(green_data['month_name'], green_data['total_co2_kgs'], color='forestgreen', alpha=0.8)

        ax2.set_xlabel('Month')
        ax2.set_ylabel('Total CO2 Emissions (kg)')
        ax2.set_title('Monthly CO2 Emissions - Green Taxi')
        ax2.set_xticks(range(len(month_names)))
        ax2.set_xticklabels(month_names, rotation=45)
        ax2.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig('taxi_co2_emissions_analysis.png', dpi=300, bbox_inches='tight')

        print("Analysis complete! Chart saved as 'taxi_co2_emissions_analysis.png'")

        logger.info("Generated CO2 emissions visualization.")

        # closing the connection
        con.close()

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    analysis_parquet()