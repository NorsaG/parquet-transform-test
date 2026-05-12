USE transform_demo;

SELECT
  case_id,
  src_ts15,
  TRIM(src_ts15) RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}(Z|[+-][0-9]{2}:[0-9]{2})\[[^\]]+\]$' AS matches_ts15_pattern,
  REGEXP_EXTRACT(TRIM(src_ts15), '(Z|[+-][0-9]{2}:[0-9]{2})', 1) AS extracted_offset,
  REGEXP_EXTRACT(TRIM(src_ts15), '\\[([^\\]]+)\\]$', 1) AS extracted_zone
FROM source_input
WHERE case_id IN ('baseline_valid', 'ts15_offset_zone_mismatch', 'ts15_without_brackets');

