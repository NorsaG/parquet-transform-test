#!/usr/bin/env bash
set -eu
set -o pipefail 2>/dev/null || true

# Ensure no active spark-shell keeps Derby metastore lock.
pkill -f SparkILoop || true
sleep 1

/opt/spark/bin/spark-sql -f /workspace/scripts/hive/select_datetime_edge_cases.sql

