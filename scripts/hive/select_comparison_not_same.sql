-- USE default;

SELECT
  case_id,
  source_value,
  transformed_value,
  etalon_value,
  comparison_result
FROM default.transformation_comparison_view
WHERE comparison_result = 'NOT SAME'
ORDER BY case_id;

