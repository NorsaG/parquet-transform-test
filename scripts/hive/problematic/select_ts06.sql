-- USE default;
WITH ranked AS (
  SELECT
    s.case_id,
    s.src_ts6 AS source_value,
    v.ts6 AS transformed_value,
    ROW_NUMBER() OVER (ORDER BY s.case_id) AS rn
  FROM default.source_input s
  LEFT JOIN default.transformed_view v ON v.case_id = s.case_id
  WHERE s.src_ts6 IS NOT NULL OR v.ts6 IS NOT NULL
)
SELECT
  case_id,
  source_value,
  transformed_value
FROM ranked
WHERE rn <= 20
ORDER BY rn;
