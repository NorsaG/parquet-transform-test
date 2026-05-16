#!/usr/bin/env bash
set -eu
set -o pipefail 2>/dev/null || true

/opt/spark/bin/spark-sql -f /workspace/scripts/hive/select_comparison_all.sql

