-- USE default;
WITH ranked AS (
  SELECT
    s.case_id,
    CAST(s.src_ts12 AS STRING) AS source_value,
    CAST(v.ts12 AS STRING) AS transformed_value,
    ROW_NUMBER() OVER (ORDER BY s.case_id) AS rn
  FROM default.source_input s
  LEFT JOIN default.transformed_view v ON v.case_id = s.case_id
  WHERE s.src_ts12 IS NOT NULL OR v.ts12 IS NOT NULL
)
SELECT
  case_id,
  source_value,
  transformed_value
FROM ranked
WHERE rn <= 14
ORDER BY rn;
