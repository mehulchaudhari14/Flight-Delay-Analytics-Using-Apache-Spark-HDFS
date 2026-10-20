#!/usr/bin/env bash
# ==============================================================================
# Script: upload_to_hdfs.sh
# Purpose: Create HDFS directory structure and upload BTS flight dataset to HDFS
# ==============================================================================

set -e

CONTAINER_NAME="flight_namenode"

echo "======================================================================"
echo " [HDFS SETUP] Initializing HDFS Directories and Uploading Dataset"
echo "======================================================================"

# 1. Wait for NameNode to be ready
echo "[*] Checking HDFS NameNode availability..."
until docker exec ${CONTAINER_NAME} hdfs dfsadmin -safemode get 2>/dev/null | grep -q "Safe mode is OFF"; do
    echo "[...] Safe mode is ON or NameNode starting up. Waiting 3 seconds..."
    docker exec ${CONTAINER_NAME} hdfs dfsadmin -safemode leave 2>/dev/null || true
    sleep 3
done
echo "[+] NameNode is active and Safe Mode is OFF!"

# 2. Create HDFS directories
echo "[*] Creating HDFS directory hierarchy (/flight_data/raw, /flight_data/cleaned, /flight_data/analytics)..."
docker exec ${CONTAINER_NAME} hdfs dfs -mkdir -p /flight_data/raw
docker exec ${CONTAINER_NAME} hdfs dfs -mkdir -p /flight_data/cleaned
docker exec ${CONTAINER_NAME} hdfs dfs -mkdir -p /flight_data/analytics

# 3. Upload dataset to HDFS
echo "[*] Uploading raw BTS flight dataset to HDFS: /flight_data/raw/T_ONTIME_REPORTING.csv"
docker exec ${CONTAINER_NAME} hdfs dfs -put -f /data/raw/T_ONTIME_REPORTING.csv /flight_data/raw/T_ONTIME_REPORTING.csv

# 4. Verify HDFS storage
echo ""
echo "======================================================================"
echo " [VERIFICATION] Verifying files stored in HDFS:"
echo "======================================================================"
docker exec ${CONTAINER_NAME} hdfs dfs -ls -h /flight_data/raw

echo ""
echo "[*] Displaying first 5 lines directly from HDFS:"
docker exec ${CONTAINER_NAME} hdfs dfs -cat /flight_data/raw/T_ONTIME_REPORTING.csv | head -n 5

echo ""
echo "[SUCCESS] BTS Flight Dataset successfully uploaded and verified in HDFS!"
echo "======================================================================"
