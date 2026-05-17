-- USE default;
SELECT COUNT(*) AS cnt FROM default.transformation_comparison_view;
SELECT case_id, source_value, transformed_value, etalon_value, comparison_result
FROM default.transformation_comparison_view
ORDER BY case_id
LIMIT 10;

