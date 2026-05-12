#!/usr/bin/env bash
set -eu
set -o pipefail 2>/dev/null || true
beeline -u "jdbc:hive2://hive-server2:10000/default" -n hive -p hive -f /workspace/scripts/hive/select_problematic_transformations_beeline.sql
