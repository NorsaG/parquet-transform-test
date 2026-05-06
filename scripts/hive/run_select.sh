#!/usr/bin/env bash
set -euo pipefail

# Ensure no active spark-shell keeps Derby metastore lock.
pkill -f SparkILoop || true
sleep 1

/opt/spark/bin/spark-sql -f /workspace/scripts/hive/select_from_hive.sql

