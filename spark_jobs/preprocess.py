"""
Flight Data Preprocessing Script using Apache Spark & HDFS
==========================================================
Cleans raw BTS flight records, handles missing/null values,
performs type casting, standardizes categorical values, enriches
features (carrier names, route, delay categories), and saves
the cleaned dataset back to HDFS in high-performance Parquet format.
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, concat_ws, lit, coalesce, trim, upper
)
from pyspark.sql.types import IntegerType, DoubleType, StringType

CARRIER_MAP = {
    "AA": "American Airlines",
    "DL": "Delta Air Lines",
    "UA": "United Airlines",
    "WN": "Southwest Airlines",
    "B6": "JetBlue Airways",
    "AS": "Alaska Airlines",
    "NK": "Spirit Airlines",
    "F9": "Frontier Airlines",
    "HA": "Hawaiian Airlines",
    "G4": "Allegiant Air",
    "OO": "SkyWest Airlines",
    "YX": "Republic Airways",
    "MQ": "Envoy Air",
    "OH": "PSA Airlines",
    "9E": "Endeavor Air",
    "EV": "ExpressJet Airlines",
    "QX": "Horizon Air",
    "C5": "CommuteAir",
    "PT": "Piedmont Airlines",
    "ZW": "Air Wisconsin"
}

MONTH_MAP = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}

DAY_MAP = {
    1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday",
    5: "Friday", 6: "Saturday", 7: "Sunday"
}

def create_spark_session():
    return (
        SparkSession.builder
        .appName("FlightDataPreprocessing")
        .config("spark.sql.shuffle.partitions", "10")
        .getOrCreate()
    )

def main():
    print("=" * 80)
    print(" [2/3] FLIGHT DELAY ANALYTICS - PREPROCESSING & DATA CLEANING JOB")
    print("=" * 80)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    hdfs_raw_path = "hdfs://namenode:9000/flight_data/raw/T_ONTIME_REPORTING.csv"
    fallback_local_path = "/data/raw/T_ONTIME_REPORTING.csv"

    # 1. Read Raw Data
    try:
        print(f"[*] Reading raw dataset from HDFS: {hdfs_raw_path}")
        df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(hdfs_raw_path)
        _ = df_raw.first()
    except Exception as e:
        print(f"[!] Warning reading from HDFS ({e}). Reading from: {fallback_local_path}")
        df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(fallback_local_path)

    initial_count = df_raw.count()
    print(f"[+] Total raw records loaded: {initial_count:,}")

    # 2. Drop Duplicates
    df_dedup = df_raw.dropDuplicates()
    dedup_count = df_dedup.count()
    duplicates_removed = initial_count - dedup_count
    print(f"[+] Duplicate records removed: {duplicates_removed:,}")

    # 3. Clean Essential Identifiers (drop if carrier, origin, or dest is null)
    df_filtered = df_dedup.filter(
        col("OP_UNIQUE_CARRIER").isNotNull() &
        col("ORIGIN").isNotNull() &
        col("DEST").isNotNull() &
        col("FL_DATE").isNotNull()
    )

    # 4. Handle Missing Values & Type Casting
    # Cast integers and doubles
    df_cleaned = df_filtered.select(
        col("YEAR").cast(IntegerType()).alias("YEAR"),
        col("MONTH").cast(IntegerType()).alias("MONTH"),
        col("DAY_OF_MONTH").cast(IntegerType()).alias("DAY_OF_MONTH"),
        col("DAY_OF_WEEK").cast(IntegerType()).alias("DAY_OF_WEEK"),
        trim(col("FL_DATE")).alias("FL_DATE"),
        upper(trim(col("OP_UNIQUE_CARRIER"))).alias("OP_UNIQUE_CARRIER"),
        upper(trim(col("ORIGIN"))).alias("ORIGIN"),
        upper(trim(col("DEST"))).alias("DEST"),
        coalesce(col("DEP_DELAY").cast(DoubleType()), lit(0.0)).alias("DEP_DELAY"),
        coalesce(col("ARR_DELAY").cast(DoubleType()), lit(0.0)).alias("ARR_DELAY"),
        coalesce(col("CANCELLED").cast(IntegerType()), lit(0)).alias("CANCELLED"),
        coalesce(trim(col("CANCELLATION_CODE")), lit("None")).alias("CANCELLATION_CODE"),
        coalesce(col("DIVERTED").cast(IntegerType()), lit(0)).alias("DIVERTED"),
        coalesce(col("DISTANCE").cast(DoubleType()), lit(0.0)).alias("DISTANCE"),
        coalesce(col("CARRIER_DELAY").cast(DoubleType()), lit(0.0)).alias("CARRIER_DELAY"),
        coalesce(col("WEATHER_DELAY").cast(DoubleType()), lit(0.0)).alias("WEATHER_DELAY"),
        coalesce(col("NAS_DELAY").cast(DoubleType()), lit(0.0)).alias("NAS_DELAY"),
        coalesce(col("SECURITY_DELAY").cast(DoubleType()), lit(0.0)).alias("SECURITY_DELAY"),
        coalesce(col("LATE_AIRCRAFT_DELAY").cast(DoubleType()), lit(0.0)).alias("LATE_AIRCRAFT_DELAY")
    )

    # 5. Feature Engineering
    # Route: ORIGIN-DEST
    df_enriched = df_cleaned.withColumn("ROUTE", concat_ws("-", col("ORIGIN"), col("DEST")))

    # FAA Standard Delayed flag: Arrival delay > 15 min and NOT cancelled
    df_enriched = df_enriched.withColumn(
        "IS_DELAYED",
        when((col("ARR_DELAY") > 15) & (col("CANCELLED") == 0), 1).otherwise(0)
    )

    # Departure delayed flag: Departure delay > 15 min and NOT cancelled
    df_enriched = df_enriched.withColumn(
        "IS_DEP_DELAYED",
        when((col("DEP_DELAY") > 15) & (col("CANCELLED") == 0), 1).otherwise(0)
    )

    # Categorize delay severity
    df_enriched = df_enriched.withColumn(
        "DELAY_CATEGORY",
        when(col("CANCELLED") == 1, lit("Cancelled"))
        .when(col("ARR_DELAY") <= 0, lit("On-Time / Early"))
        .when(col("ARR_DELAY") <= 15, lit("Minor Delay (1-15 min)"))
        .when(col("ARR_DELAY") <= 45, lit("Moderate Delay (16-45 min)"))
        .otherwise(lit("Severe Delay (>45 min)"))
    )

    # Map Carrier Full Name
    carrier_expr = when(col("OP_UNIQUE_CARRIER") == "AA", lit("American Airlines"))
    for code, name in CARRIER_MAP.items():
        if code != "AA":
            carrier_expr = carrier_expr.when(col("OP_UNIQUE_CARRIER") == code, lit(name))
    carrier_expr = carrier_expr.otherwise(col("OP_UNIQUE_CARRIER"))
    df_enriched = df_enriched.withColumn("CARRIER_NAME", carrier_expr)

    # Map Month Name
    month_expr = when(col("MONTH") == 1, lit("January"))
    for m_num, m_name in MONTH_MAP.items():
        if m_num != 1:
            month_expr = month_expr.when(col("MONTH") == m_num, lit(m_name))
    month_expr = month_expr.otherwise(lit("Unknown"))
    df_enriched = df_enriched.withColumn("MONTH_NAME", month_expr)

    # Map Day of Week Name
    day_expr = when(col("DAY_OF_WEEK") == 1, lit("Monday"))
    for d_num, d_name in DAY_MAP.items():
        if d_num != 1:
            day_expr = day_expr.when(col("DAY_OF_WEEK") == d_num, lit(d_name))
    day_expr = day_expr.otherwise(lit("Unknown"))
    df_enriched = df_enriched.withColumn("DAY_NAME", day_expr)

    final_count = df_enriched.count()
    print(f"[+] Total Cleaned Records Ready : {final_count:,}")

    # 6. Save Cleaned Dataset to HDFS (Parquet)
    hdfs_cleaned_path = "hdfs://namenode:9000/flight_data/cleaned/flight_data_cleaned.parquet"
    local_cleaned_path = "/data/cleaned/flight_data_cleaned.parquet"

    print(f"\n[*] Writing cleaned dataset to HDFS Parquet storage: {hdfs_cleaned_path}")
    try:
        df_enriched.write.mode("overwrite").parquet(hdfs_cleaned_path)
        print("[+] Cleaned dataset successfully saved to HDFS!")
    except Exception as e:
        print(f"[!] Warning writing to HDFS ({e}). Writing locally to {local_cleaned_path}")
        df_enriched.write.mode("overwrite").parquet(local_cleaned_path)

    # Also save a sample for quick verification and local access
    try:
        os.makedirs("/data/cleaned", exist_ok=True)
        sample_df = df_enriched.limit(5000)
        sample_df.write.mode("overwrite").parquet("/data/cleaned/sample.parquet")
        print("[+] Local sample parquet created at /data/cleaned/sample.parquet")
    except Exception as e:
        print(f"[*] Note on local sample: {e}")

    print("\n" + "=" * 80)
    print(" PREPROCESSED DATASET SAMPLE (FIRST 5 ROWS)")
    print("=" * 80)
    df_enriched.select(
        "FL_DATE", "CARRIER_NAME", "ROUTE", "DEP_DELAY", "ARR_DELAY", "IS_DELAYED", "DELAY_CATEGORY"
    ).show(5, truncate=False)

    print("[SUCCESS] Data Preprocessing and Feature Engineering completed successfully!\n")
    spark.stop()

if __name__ == "__main__":
    main()
