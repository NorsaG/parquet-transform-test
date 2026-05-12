USE transform_demo;
WITH ranked AS (
  SELECT
    s.case_id,
    s.src_ts9 AS source_value,
    v.ts9 AS transformed_value,
    ROW_NUMBER() OVER (ORDER BY s.case_id) AS rn
  FROM source_input s
  LEFT JOIN transformed_view v ON v.case_id = s.case_id
  WHERE s.src_ts9 IS NOT NULL OR v.ts9 IS NOT NULL
)
SELECT
  case_id,
  source_value,
  transformed_value
FROM ranked
WHERE rn <= 20
ORDER BY rn;
