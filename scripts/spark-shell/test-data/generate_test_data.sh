#!/usr/bin/env bash
set -eu
set -o pipefail 2>/dev/null || true

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
OUT_DIR="$BASE_DIR/generated"
OUT_FILE="$OUT_DIR/all_cases.tsv"

mkdir -p "$OUT_DIR"
: > "$OUT_FILE"

for f in "$BASE_DIR"/TS*.cases; do
  [ -f "$f" ] || continue
  ts_name="$(basename "$f" .cases)"
  while IFS='|' read -r case_name raw_value; do
    case_name="${case_name%$'\r'}"
    raw_value="${raw_value%$'\r'}"
    [ -z "${case_name//[[:space:]]/}" ] && continue
    case "$case_name" in
      \#*) continue ;;
    esac
    printf '%s\t%s\t%s\n' "$ts_name" "$case_name" "$raw_value" >> "$OUT_FILE"
  done < "$f"
done

wc -l "$OUT_FILE" | awk '{print "[INFO] Generated rows: "$1}'


