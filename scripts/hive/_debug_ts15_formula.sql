USE transform_demo;

SELECT
  case_id,
  DATE_FORMAT(
    FROM_UTC_TIMESTAMP(
      TO_UTC_TIMESTAMP(
        TO_TIMESTAMP(CONCAT(SUBSTR(TRIM(src_ts15),1,10), ' ', SUBSTR(TRIM(src_ts15),12,8), '.', SUBSTR(TRIM(src_ts15),21,6)), 'yyyy-MM-dd HH:mm:ss.SSSSSS'),
        CASE
          WHEN REGEXP_EXTRACT(TRIM(src_ts15), '(Z|[+-][0-9]{2}:[0-9]{2})', 1) = 'Z' THEN '+00:00'
          ELSE REGEXP_EXTRACT(TRIM(src_ts15), '(Z|[+-][0-9]{2}:[0-9]{2})', 1)
        END
      ),
      REGEXP_EXTRACT(TRIM(src_ts15), '\\[(.+)\\]$', 1)
    ),
    'yyyy-MM-dd HH:mm:ss.SSSSSS'
  ) AS computed_ts15,
  DATE_FORMAT(
    TO_UTC_TIMESTAMP(
      TO_TIMESTAMP(CONCAT(SUBSTR(TRIM(src_ts16),1,10), ' ', SUBSTR(TRIM(src_ts16),12,8), '.', SUBSTR(TRIM(src_ts16),21,6)), 'yyyy-MM-dd HH:mm:ss.SSSSSS'),
      CASE
        WHEN REGEXP_EXTRACT(TRIM(src_ts16), '(Z|[+-][0-9]{2}:[0-9]{2})', 1) = 'Z' THEN '+00:00'
        ELSE REGEXP_EXTRACT(TRIM(src_ts16), '(Z|[+-][0-9]{2}:[0-9]{2})', 1)
      END
    ),
    'yyyy-MM-dd HH:mm:ss.SSSSSS'
  ) AS computed_ts16
FROM source_input
WHERE case_id IN ('baseline_valid', 'ts15_offset_zone_mismatch', 'ts16_india');

