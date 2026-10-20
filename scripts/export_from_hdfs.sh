#!/usr/bin/env bash
# ==============================================================================
# Script: export_from_hdfs.sh
# Purpose: Merge & extract analytical results from HDFS to local /data/analytics
#          so the Streamlit dashboard can instantly load the aggregated metrics.
# ==============================================================================

set -e

NAMENODE_CONTAINER="flight_namenode"

echo "======================================================================"
echo "    EXPORTING SPARK ANALYTICAL RESULTS FROM HDFS TO LOCAL CSV"
echo "======================================================================"

ANALYTICS_DIRS=(
    "kpi_metrics"
    "airline_delays"
    "monthly_trends"
    "daily_trends"
    "airport_delays"
    "route_delays"
    "delay_causes"
    "cancellation_stats"
    "flight_sample"
)

# Create /data/analytics inside the container volume
docker exec ${NAMENODE_CONTAINER} mkdir -p /data/analytics

for dir in "${ANALYTICS_DIRS[@]}"; do
    echo "[*] Checking HDFS path: /flight_data/analytics/${dir}..."
    if docker exec ${NAMENODE_CONTAINER} hdfs dfs -test -e "/flight_data/analytics/${dir}"; then
        echo "    -> Exporting to /data/analytics/${dir}.csv"
        docker exec ${NAMENODE_CONTAINER} rm -f "/data/analytics/${dir}.csv"
        docker exec ${NAMENODE_CONTAINER} hdfs dfs -getmerge "/flight_data/analytics/${dir}" "/data/analytics/${dir}.csv"
    else
        echo "    -> Path not found in HDFS, skipping ${dir}."
    fi
done

echo ""
echo "[+] Listing exported analytics CSVs in /data/analytics:"
docker exec ${NAMENODE_CONTAINER} ls -lh /data/analytics

echo ""
echo "======================================================================"
echo " [SUCCESS] All analytical results successfully synced from HDFS!"
echo "======================================================================"
