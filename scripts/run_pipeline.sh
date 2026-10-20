#!/usr/bin/env bash
# ==============================================================================
# Script: run_pipeline.sh
# Purpose: Execute the complete Big Data pipeline end-to-end:
#          Upload -> Spark Ingest -> Spark Preprocess -> Spark SQL Analytics
# ==============================================================================

set -e

SPARK_CONTAINER="spark-master"
NAMENODE_CONTAINER="flight_namenode"

echo "======================================================================"
echo "    FLIGHT DELAY ANALYTICS USING APACHE SPARK - COMPLETE PIPELINE"
echo "======================================================================"

# Step 1: Upload dataset to HDFS
echo ""
echo ">>> STEP 1: Uploading Flight Dataset to HDFS..."
bash scripts/upload_to_hdfs.sh

# Step 2: Ingest & Inspect
echo ""
echo ">>> STEP 2: Running Spark Ingestion Job (Validating Schema & Records)..."
docker exec ${SPARK_CONTAINER} /opt/spark/bin/spark-submit \
    --master local[*] \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
    /opt/spark_jobs/ingest_flight_data.py

# Step 3: Preprocess & Clean
echo ""
echo ">>> STEP 3: Running Spark Preprocessing Job (Cleaning & Parquet Storage in HDFS)..."
docker exec ${SPARK_CONTAINER} /opt/spark/bin/spark-submit \
    --master local[*] \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
    /opt/spark_jobs/preprocess.py

# Step 4: Spark SQL Analytics
echo ""
echo ">>> STEP 4: Running Spark SQL Analytics Job (Aggregating Metrics into HDFS)..."
docker exec ${SPARK_CONTAINER} /opt/spark/bin/spark-submit \
    --master local[*] \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
    /opt/spark_jobs/flight_analytics.py

# Step 5: Export Analytics from HDFS to Local Dashboard Storage
echo ""
echo ">>> STEP 5: Exporting Analytics Outputs from HDFS for Streamlit Dashboard..."
bash scripts/export_from_hdfs.sh

# Step 6: Verify HDFS Results
echo ""
echo "======================================================================"
echo ">>> STEP 6: Verifying Analytics Output in HDFS:"
echo "======================================================================"
docker exec ${NAMENODE_CONTAINER} hdfs dfs -ls -h /flight_data/analytics

echo ""
echo "======================================================================"
echo " [SUCCESS] PIPELINE EXECUTION FINISHED SUCCESSFULLY!"
echo "======================================================================"
echo " Access the Services:"
echo " - Streamlit Dashboard : http://localhost:8501"
echo " - HDFS NameNode Web UI: http://localhost:9870"
echo " - Spark Master Web UI : http://localhost:8085"
echo "======================================================================"
