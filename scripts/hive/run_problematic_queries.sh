#!/usr/bin/env bash
set -eu
set -o pipefail 2>/dev/null || true

echo "[INFO] Running /workspace/scripts/hive/select_problematic_queries_clear.sql"
/opt/spark/bin/spark-sql -f /workspace/scripts/hive/select_problematic_queries_clear.sql
echo "[INFO] Completed /workspace/scripts/hive/select_problematic_queries_clear.sql"

