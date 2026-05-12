#!/usr/bin/env bash
set -eu
set -o pipefail 2>/dev/null || true

/opt/spark/bin/spark-shell <<'EOF'
:load /workspace/scripts/spark-shell/pipeline.scala
:quit
EOF
