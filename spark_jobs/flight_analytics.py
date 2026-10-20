"""
Flight Delay Analytics using Apache Spark & Spark SQL
=====================================================
Executes Big Data analytics queries on cleaned flight dataset from HDFS.
Performs:
  1. Overall KPI Metrics Calculation
  2. Airline-wise Delays and On-Time Performance
  3. Monthly and Day-of-Week Flight Delay Trends
  4. Airport-wise Departure/Arrival Delays
  5. Route-wise Performance Analytics
  6. Root Cause Decomposition of Delays (Carrier, Weather, NAS, Security, Late Aircraft)
  7. Cancellation and Diversion Statistics

Saves all analytical results back to HDFS and exports CSVs to /data/analytics.
"""

import sys
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, round, when, desc, asc, lit
)

def create_spark_session():
    return (
        SparkSession.builder
        .appName("FlightDelayAnalyticsSparkSQL")
        .config("spark.sql.shuffle.partitions", "10")
        .getOrCreate()
    )

def save_and_export_df(spark, df, hdfs_dir, local_csv_path):
    """
    Saves dataframe to HDFS as CSV and exports a single clean CSV
    to local /data/analytics folder for instantaneous dashboard loading.
    Uses Spark-native CSV writer (no pandas dependency).
    """
    # 1. Save to HDFS
    try:
        df.write.mode("overwrite").option("header", "true").csv(hdfs_dir)
        print(f" [HDFS] Saved aggregated results to: {hdfs_dir}")
    except Exception as e:
        print(f" [!] HDFS save notice for {hdfs_dir}: {e}")

    # 2. Export clean consolidated CSV for dashboard (using Spark native writer)
    try:
        os.makedirs(os.path.dirname(local_csv_path), exist_ok=True)
        # Use file:// protocol to ensure Spark writes to local filesystem instead of HDFS defaultFS
        tmp_dir = "file://" + local_csv_path + "_tmp"
        local_tmp_dir = local_csv_path + "_tmp"
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp_dir)
        # Find the part file and rename it to the target CSV
        import glob
        part_files = glob.glob(os.path.join(local_tmp_dir, "part-*.csv"))
        if part_files:
            shutil.move(part_files[0], local_csv_path)
        shutil.rmtree(local_tmp_dir, ignore_errors=True)
        print(f" [LOCAL] Exported analytical CSV to: {local_csv_path}")
    except Exception as e:
        print(f" [!] Local export error for {local_csv_path}: {e}")

def main():
    print("=" * 80)
    print(" [3/3] FLIGHT DELAY ANALYTICS - SPARK SQL ANALYTICS PIPELINE")
    print("=" * 80)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Read cleaned data from HDFS
    hdfs_cleaned_path = "hdfs://namenode:9000/flight_data/cleaned/flight_data_cleaned.parquet"
    local_cleaned_path = "/data/cleaned/flight_data_cleaned.parquet"

    try:
        print(f"[*] Reading cleaned dataset from HDFS: {hdfs_cleaned_path}")
        df = spark.read.parquet(hdfs_cleaned_path)
        _ = df.first()
    except Exception as e:
        print(f"[!] Warning reading from HDFS ({e}). Reading from local: {local_cleaned_path}")
        df = spark.read.parquet(local_cleaned_path)

    # Register Temp View for Spark SQL
    df.createOrReplaceTempView("flights")
    print(f"[+] Loaded {df.count():,} cleaned flight records into Spark SQL view 'flights'\n")

    # -------------------------------------------------------------------------
    # 1. OVERALL KPI METRICS
    # -------------------------------------------------------------------------
    print("=" * 80)
    print(" 1. OVERALL BIG DATA KPI SUMMARY")
    print("=" * 80)
    kpi_df = spark.sql("""
        SELECT 
            COUNT(*) AS total_flights,
            SUM(CASE WHEN CANCELLED = 0 THEN 1 ELSE 0 END) AS completed_flights,
            SUM(IS_DELAYED) AS delayed_flights,
            ROUND(SUM(IS_DELAYED) * 100.0 / NULLIF(SUM(CASE WHEN CANCELLED = 0 THEN 1 ELSE 0 END), 0), 2) AS delayed_percentage,
            ROUND(AVG(CASE WHEN CANCELLED = 0 THEN ARR_DELAY ELSE NULL END), 2) AS avg_arrival_delay,
            ROUND(AVG(CASE WHEN CANCELLED = 0 THEN DEP_DELAY ELSE NULL END), 2) AS avg_departure_delay,
            MAX(ARR_DELAY) AS max_arrival_delay,
            SUM(CANCELLED) AS cancelled_flights,
            ROUND(SUM(CANCELLED) * 100.0 / COUNT(*), 2) AS cancellation_rate,
            SUM(DIVERTED) AS diverted_flights,
            ROUND(SUM(DIVERTED) * 100.0 / COUNT(*), 2) AS diversion_rate,
            ROUND(SUM(DISTANCE), 2) AS total_distance_miles
        FROM flights
    """)
    kpi_df.show(truncate=False)
    save_and_export_df(spark, kpi_df, "hdfs://namenode:9000/flight_data/analytics/kpi_metrics", "/data/analytics/kpi_metrics.csv")

    # -------------------------------------------------------------------------
    # 2. AIRLINE-WISE DELAY PERFORMANCE
    # -------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print(" 2. AIRLINE-WISE PERFORMANCE & DELAY COMPARISON")
    print("=" * 80)
    airline_df = spark.sql("""
        SELECT 
            OP_UNIQUE_CARRIER AS carrier_code,
            CARRIER_NAME AS carrier_name,
            COUNT(*) AS total_flights,
            SUM(IS_DELAYED) AS delayed_flights,
            ROUND(SUM(IS_DELAYED) * 100.0 / NULLIF(SUM(CASE WHEN CANCELLED = 0 THEN 1 ELSE 0 END), 0), 2) AS delay_rate_pct,
            ROUND(100.0 - (SUM(IS_DELAYED) * 100.0 / NULLIF(SUM(CASE WHEN CANCELLED = 0 THEN 1 ELSE 0 END), 0)), 2) AS on_time_rate_pct,
            ROUND(AVG(CASE WHEN CANCELLED = 0 THEN DEP_DELAY ELSE NULL END), 2) AS avg_dep_delay_min,
            ROUND(AVG(CASE WHEN CANCELLED = 0 THEN ARR_DELAY ELSE NULL END), 2) AS avg_arr_delay_min,
            SUM(CANCELLED) AS cancelled_flights,
            ROUND(SUM(CANCELLED) * 100.0 / COUNT(*), 2) AS cancellation_rate_pct,
            SUM(DIVERTED) AS diverted_flights
        FROM flights
        GROUP BY OP_UNIQUE_CARRIER, CARRIER_NAME
        ORDER BY total_flights DESC
    """)
    airline_df.show(20, truncate=False)
    save_and_export_df(spark, airline_df, "hdfs://namenode:9000/flight_data/analytics/airline_delays", "/data/analytics/airline_delays.csv")

    # -------------------------------------------------------------------------
    # 3. MONTHLY AND DAY-OF-WEEK DELAY TRENDS
    # -------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print(" 3. MONTHLY & DAY-OF-WEEK FLIGHT DELAY TRENDS")
    print("=" * 80)
    monthly_df = spark.sql("""
        SELECT 
            MONTH AS month_num,
            MONTH_NAME AS month_name,
            COUNT(*) AS total_flights,
            SUM(IS_DELAYED) AS delayed_flights,
            ROUND(SUM(IS_DELAYED) * 100.0 / NULLIF(SUM(CASE WHEN CANCELLED = 0 THEN 1 ELSE 0 END), 0), 2) AS delay_rate_pct,
            ROUND(AVG(CASE WHEN CANCELLED = 0 THEN ARR_DELAY ELSE NULL END), 2) AS avg_arr_delay_min,
            ROUND(AVG(CASE WHEN CANCELLED = 0 THEN DEP_DELAY ELSE NULL END), 2) AS avg_dep_delay_min,
            SUM(CANCELLED) AS cancelled_flights
        FROM flights
        GROUP BY MONTH, MONTH_NAME
        ORDER BY MONTH ASC
    """)
    monthly_df.show(truncate=False)
    save_and_export_df(spark, monthly_df, "hdfs://namenode:9000/flight_data/analytics/monthly_trends", "/data/analytics/monthly_trends.csv")

    daily_df = spark.sql("""
        SELECT 
            DAY_OF_WEEK AS day_of_week_num,
            DAY_NAME AS day_name,
            COUNT(*) AS total_flights,
            SUM(IS_DELAYED) AS delayed_flights,
            ROUND(SUM(IS_DELAYED) * 100.0 / NULLIF(SUM(CASE WHEN CANCELLED = 0 THEN 1 ELSE 0 END), 0), 2) AS delay_rate_pct,
            ROUND(AVG(CASE WHEN CANCELLED = 0 THEN ARR_DELAY ELSE NULL END), 2) AS avg_arr_delay_min,
            ROUND(AVG(CASE WHEN CANCELLED = 0 THEN DEP_DELAY ELSE NULL END), 2) AS avg_dep_delay_min,
            SUM(CANCELLED) AS cancelled_flights
        FROM flights
        GROUP BY DAY_OF_WEEK, DAY_NAME
        ORDER BY DAY_OF_WEEK ASC
    """)
    daily_df.show(truncate=False)
    save_and_export_df(spark, daily_df, "hdfs://namenode:9000/flight_data/analytics/daily_trends", "/data/analytics/daily_trends.csv")

    # -------------------------------------------------------------------------
    # 4. AIRPORT-WISE DELAY PERFORMANCE
    # -------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print(" 4. AIRPORT-WISE PERFORMANCE (ORIGIN & DESTINATION)")
    print("=" * 80)
    airport_df = spark.sql("""
        SELECT 
            ORIGIN AS airport_code,
            COUNT(*) AS total_departures,
            SUM(IS_DEP_DELAYED) AS delayed_departures,
            ROUND(SUM(IS_DEP_DELAYED) * 100.0 / NULLIF(SUM(CASE WHEN CANCELLED = 0 THEN 1 ELSE 0 END), 0), 2) AS dep_delay_rate_pct,
            ROUND(AVG(CASE WHEN CANCELLED = 0 THEN DEP_DELAY ELSE NULL END), 2) AS avg_dep_delay_min,
            ROUND(AVG(CASE WHEN CANCELLED = 0 THEN ARR_DELAY ELSE NULL END), 2) AS avg_arr_delay_min,
            SUM(CANCELLED) AS cancelled_departures
        FROM flights
        GROUP BY ORIGIN
        HAVING COUNT(*) >= 50
        ORDER BY total_departures DESC
    """)
    airport_df.show(15, truncate=False)
    save_and_export_df(spark, airport_df, "hdfs://namenode:9000/flight_data/analytics/airport_delays", "/data/analytics/airport_delays.csv")

    # -------------------------------------------------------------------------
    # 5. ROUTE-WISE DELAY PERFORMANCE
    # -------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print(" 5. ROUTE-WISE PERFORMANCE (ORIGIN TO DESTINATION)")
    print("=" * 80)
    route_df = spark.sql("""
        SELECT 
            ROUTE AS route,
            ORIGIN AS origin,
            DEST AS dest,
            COUNT(*) AS total_flights,
            SUM(IS_DELAYED) AS delayed_flights,
            ROUND(SUM(IS_DELAYED) * 100.0 / NULLIF(SUM(CASE WHEN CANCELLED = 0 THEN 1 ELSE 0 END), 0), 2) AS delay_rate_pct,
            ROUND(AVG(CASE WHEN CANCELLED = 0 THEN ARR_DELAY ELSE NULL END), 2) AS avg_arr_delay_min,
            ROUND(AVG(DISTANCE), 0) AS distance_miles
        FROM flights
        GROUP BY ROUTE, ORIGIN, DEST
        HAVING COUNT(*) >= 30
        ORDER BY total_flights DESC
    """)
    route_df.show(15, truncate=False)
    save_and_export_df(spark, route_df, "hdfs://namenode:9000/flight_data/analytics/route_delays", "/data/analytics/route_delays.csv")

    # -------------------------------------------------------------------------
    # 6. DELAY CAUSE DECOMPOSITION
    # -------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print(" 6. ROOT CAUSE DELAY DECOMPOSITION (CARRIER, WEATHER, NAS, SECURITY, LATE AIRCRAFT)")
    print("=" * 80)
    causes_df = spark.sql("""
        SELECT 
            'Carrier Delay (Airline Operation/Crew)' AS delay_cause,
            ROUND(SUM(CARRIER_DELAY), 0) AS total_delay_minutes,
            COUNT(CASE WHEN CARRIER_DELAY > 0 THEN 1 ELSE NULL END) AS affected_flights
        FROM flights
        UNION ALL
        SELECT 
            'Weather Delay (Extreme Weather)' AS delay_cause,
            ROUND(SUM(WEATHER_DELAY), 0) AS total_delay_minutes,
            COUNT(CASE WHEN WEATHER_DELAY > 0 THEN 1 ELSE NULL END) AS affected_flights
        FROM flights
        UNION ALL
        SELECT 
            'NAS Delay (Air Traffic Control / Airport)' AS delay_cause,
            ROUND(SUM(NAS_DELAY), 0) AS total_delay_minutes,
            COUNT(CASE WHEN NAS_DELAY > 0 THEN 1 ELSE NULL END) AS affected_flights
        FROM flights
        UNION ALL
        SELECT 
            'Security Delay (Terminal / Screening)' AS delay_cause,
            ROUND(SUM(SECURITY_DELAY), 0) AS total_delay_minutes,
            COUNT(CASE WHEN SECURITY_DELAY > 0 THEN 1 ELSE NULL END) AS affected_flights
        FROM flights
        UNION ALL
        SELECT 
            'Late Aircraft Delay (Ripple Effect)' AS delay_cause,
            ROUND(SUM(LATE_AIRCRAFT_DELAY), 0) AS total_delay_minutes,
            COUNT(CASE WHEN LATE_AIRCRAFT_DELAY > 0 THEN 1 ELSE NULL END) AS affected_flights
        FROM flights
    """)
    
    # Calculate percentage share
    total_cause_minutes = causes_df.agg(sum("total_delay_minutes")).collect()[0][0] or 1.0
    causes_enriched = causes_df.withColumn(
        "percentage_share",
        round((col("total_delay_minutes") * 100.0) / lit(total_cause_minutes), 2)
    ).orderBy(desc("total_delay_minutes"))
    
    causes_enriched.show(truncate=False)
    save_and_export_df(spark, causes_enriched, "hdfs://namenode:9000/flight_data/analytics/delay_causes", "/data/analytics/delay_causes.csv")

    # -------------------------------------------------------------------------
    # 7. CANCELLATION & DIVERSION STATISTICS
    # -------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print(" 7. CANCELLATION & DIVERSION STATISTICS")
    print("=" * 80)
    cancel_df = spark.sql("""
        SELECT 
            CASE 
                WHEN CANCELLATION_CODE = 'A' THEN 'Carrier Cancellation (A)'
                WHEN CANCELLATION_CODE = 'B' THEN 'Weather Cancellation (B)'
                WHEN CANCELLATION_CODE = 'C' THEN 'NAS / Air Traffic Cancellation (C)'
                WHEN CANCELLATION_CODE = 'D' THEN 'Security Cancellation (D)'
                ELSE 'Other / Not Cancelled'
            END AS cancellation_reason,
            COUNT(*) AS flight_count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM flights WHERE CANCELLED = 1), 2) AS pct_of_cancellations
        FROM flights
        WHERE CANCELLED = 1
        GROUP BY CANCELLATION_CODE
        ORDER BY flight_count DESC
    """)
    cancel_df.show(truncate=False)
    save_and_export_df(spark, cancel_df, "hdfs://namenode:9000/flight_data/analytics/cancellation_stats", "/data/analytics/cancellation_stats.csv")

    # -------------------------------------------------------------------------
    # 8. EXPORT CLEANED FLIGHT SAMPLE FOR DASHBOARD DRILL-DOWN
    # -------------------------------------------------------------------------
    print("\n[*] Exporting sample records for interactive dashboard raw drill-down...")
    sample_export = df.select(
        "FL_DATE", "OP_UNIQUE_CARRIER", "CARRIER_NAME", "ORIGIN", "DEST", "ROUTE",
        "DEP_DELAY", "ARR_DELAY", "IS_DELAYED", "DELAY_CATEGORY", "CANCELLED", "DIVERTED"
    ).limit(5000)
    save_and_export_df(spark, sample_export, "hdfs://namenode:9000/flight_data/analytics/flight_sample", "/data/analytics/flight_sample.csv")
    print("[+] Saved 5,000 flight sample records to HDFS and /data/analytics/flight_sample.csv")

    print("\n" + "=" * 80)
    print(" [SUCCESS] ALL SPARK SQL BIG DATA ANALYTICS JOBS COMPLETED SUCCESSFULLY!")
    print("=" * 80 + "\n")
    spark.stop()

if __name__ == "__main__":
    main()
