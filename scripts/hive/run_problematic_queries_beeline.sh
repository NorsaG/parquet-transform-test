#!/usr/bin/env bash
set -eu
set -o pipefail 2>/dev/null || true

echo "[INFO] Running /workspace/scripts/hive/select_problematic_queries_clear.sql"
beeline -u "jdbc:hive2://hive-server2:10000/default" -n hive -p hive -f /workspace/scripts/hive/select_problematic_queries_clear.sql
echo "[INFO] Completed /workspace/scripts/hive/select_problematic_queries_clear.sql"

