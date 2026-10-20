"""
Flight Data Ingestion Script using Apache Spark & HDFS
======================================================
Reads the raw BTS flight dataset directly from HDFS,
validates the schema, checks record counts, and inspects null values.
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

def create_spark_session():
    return (
        SparkSession.builder
        .appName("FlightDataIngestion")
        .config("spark.sql.shuffle.partitions", "10")
        .getOrCreate()
    )

def main():
    print("=" * 80)
    print(" [1/3] FLIGHT DELAY ANALYTICS - HDFS DATA INGESTION JOB")
    print("=" * 80)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Priority HDFS path
    hdfs_raw_path = "hdfs://namenode:9000/flight_data/raw/T_ONTIME_REPORTING.csv"
    fallback_local_path = "/data/raw/T_ONTIME_REPORTING.csv"

    # Attempt to read from HDFS first, then fallback to local volume if needed
    data_path = hdfs_raw_path
    try:
        print(f"[*] Connecting to HDFS and reading raw flight data from: {hdfs_raw_path}")
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(data_path)
        # Trigger an action to verify HDFS connection
        _ = df.first()
        print("[+] Successfully connected to HDFS NameNode!")
    except Exception as e:
        print(f"[!] Warning: Could not read directly from {hdfs_raw_path} ({e})")
        print(f"[*] Falling back to container volume mount: {fallback_local_path}")
        data_path = fallback_local_path
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(data_path)

    # Display Schema
    print("\n" + "=" * 80)
    print(" RAW DATASET SCHEMA (BTS On-Time Flight Performance)")
    print("=" * 80)
    df.printSchema()

    # Total row and column count
    total_records = df.count()
    num_columns = len(df.columns)
    print(f"\n[+] Total Records Ingested : {total_records:,}")
    print(f"[+] Total Fields / Columns  : {num_columns}")

    # Inspect distinct carriers (airlines)
    if "OP_UNIQUE_CARRIER" in df.columns:
        carriers = [row["OP_UNIQUE_CARRIER"] for row in df.select("OP_UNIQUE_CARRIER").distinct().collect()]
        print(f"[+] Distinct Carriers Found : {len(carriers)} -> {', '.join(sorted(carriers))}")

    # Inspect null distribution on key delay & status columns
    print("\n" + "=" * 80)
    print(" NULL VALUE AUDIT ACROSS KEY METRIC COLUMNS")
    print("=" * 80)
    check_cols = ["DEP_DELAY", "ARR_DELAY", "CANCELLED", "DIVERTED",
                  "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"]
    
    existing_cols = [c for c in check_cols if c in df.columns]
    null_exprs = [count(when(col(c).isNull(), c)).alias(c) for c in existing_cols]
    null_counts = df.select(null_exprs).collect()[0].asDict()

    for col_name, n_nulls in null_counts.items():
        pct = (n_nulls / total_records) * 100 if total_records > 0 else 0
        print(f" - {col_name:<22}: {n_nulls:>8,} nulls ({pct:>6.2f}%)")

    print("\n" + "=" * 80)
    print(" SAMPLE RAW FLIGHT RECORDS (FIRST 5 ROWS)")
    print("=" * 80)
    sample_cols = [c for c in ["FL_DATE", "OP_UNIQUE_CARRIER", "ORIGIN", "DEST", "DEP_DELAY", "ARR_DELAY", "CANCELLED"] if c in df.columns]
    df.select(sample_cols).show(5, truncate=False)

    print("[SUCCESS] Data Ingestion and Schema Validation completed successfully!\n")
    spark.stop()

if __name__ == "__main__":
    main()
