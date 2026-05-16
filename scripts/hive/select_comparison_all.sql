USE transform_demo;

SELECT
  case_id,
  source_value,
  transformed_value,
  etalon_value,
  comparison_result
FROM transformation_comparison_view
ORDER BY case_id;

