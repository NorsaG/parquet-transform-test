USE transform_demo;

SELECT
  case_id,
  ts9,
  CASE
    WHEN case_id = 'ts9_overflow' AND ts9 IS NULL THEN 'PASS_EXPECTED_NULL'
    ELSE 'INFO'
  END AS ts9_check,
  ts10,
  CASE
    WHEN case_id = 'ts10_nan' AND ts10 IS NULL THEN 'PASS_EXPECTED_NULL'
    ELSE 'INFO'
  END AS ts10_check,
  ts12,
  CASE
    WHEN case_id = 'ts12_positive_infinity' AND ts12 IS NULL THEN 'PASS_EXPECTED_NULL'
    ELSE 'INFO'
  END AS ts12_check,
  ts19,
  CASE
    WHEN case_id = 'ts19_without_colon' AND ts19 IS NULL THEN 'PASS_EXPECTED_NULL'
    ELSE 'INFO'
  END AS ts19_check,
  ts20,
  CASE
    WHEN case_id = 'ts20_empty_array' AND ts20 = '' THEN 'PASS'
    WHEN case_id = 'ts20_invalid_json' AND ts20 IS NULL THEN 'PASS_EXPECTED_NULL'
    ELSE 'INFO'
  END AS ts20_check,
  ts21,
  CASE
    WHEN case_id = 'ts21_empty_array' AND ts21 = '' THEN 'PASS'
    WHEN case_id = 'ts21_only_nulls' AND ts21 = 'null|null' THEN 'PASS'
    ELSE 'INFO'
  END AS ts21_check
FROM transformed_view
WHERE case_id IN (
  'ts9_overflow',
  'ts10_nan',
  'ts12_positive_infinity',
  'ts19_without_colon',
  'ts20_empty_array',
  'ts20_invalid_json',
  'ts21_empty_array',
  'ts21_only_nulls'
)
ORDER BY case_id;

