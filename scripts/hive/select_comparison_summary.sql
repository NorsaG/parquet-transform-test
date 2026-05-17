-- USE default;

SELECT
  LOWER(REGEXP_EXTRACT(case_id, '^(ts[0-9]{2})_', 1)) AS transformation,
  COUNT(*) AS total_cases,
  SUM(CASE WHEN comparison_result = 'SAME' THEN 1 ELSE 0 END) AS same_cases,
  SUM(CASE WHEN comparison_result = 'NOT SAME' THEN 1 ELSE 0 END) AS not_same_cases
FROM default.transformation_comparison_view
GROUP BY LOWER(REGEXP_EXTRACT(case_id, '^(ts[0-9]{2})_', 1))
ORDER BY transformation;

