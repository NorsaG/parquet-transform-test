#!/usr/bin/env bash
set -euo pipefail

/opt/spark/bin/spark-shell <<'EOF'
:load /workspace/scripts/spark-shell/read_source.scala
:quit
EOF

