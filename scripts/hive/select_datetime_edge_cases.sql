-- USE default;

SELECT
  case_id,
  ts1,
  CASE
    WHEN ts1 >= TIMESTAMP '0001-01-01 00:00:00' AND ts1 <= TIMESTAMP '9999-12-31 23:59:59'
      THEN 'PASS_IN_RANGE'
    ELSE 'FAIL_OUT_OF_RANGE_OR_NULL'
  END AS ts1_range_check,

  ts4,
  CASE
    WHEN ts4 >= TIMESTAMP '0001-01-01 00:00:00' AND ts4 <= TIMESTAMP '9999-12-31 23:59:59'
      THEN 'PASS_IN_RANGE'
    ELSE 'FAIL_OUT_OF_RANGE_OR_NULL'
  END AS ts4_range_check,

  ts5,
  CASE
    WHEN case_id = 'datetime_string_out_of_range' AND ts5 IS NULL THEN 'PASS_EXPECTED_NULL'
    WHEN case_id IN ('datetime_min_boundary', 'datetime_max_boundary') AND ts5 IS NOT NULL THEN 'PASS_EXPECTED_VALUE'
    ELSE 'INFO'
  END AS ts5_check,

  ts6,
  CASE
    WHEN case_id = 'ts6_offset_0530' AND ts6 = '2026-04-13 13:42:43.271025+05:30' THEN 'PASS'
    WHEN case_id = 'ts6_offset_0300_textzone' AND ts6 = '2026-04-13 13:42:43.271025+03' THEN 'PASS'
    WHEN case_id = 'ts6_invalid_short_fraction' AND ts6 IS NULL THEN 'PASS_EXPECTED_NULL'
    ELSE 'INFO'
  END AS ts6_check,

  ts13,
  CASE
    WHEN case_id = 'ts13_invalid_no_offset' AND ts13 IS NULL THEN 'PASS_EXPECTED_NULL'
    ELSE 'INFO'
  END AS ts13_check,

  ts14,
  CASE
    WHEN case_id = 'ts14_day_shift_forward' AND ts14 = '2024-05-21 04:00:00.000000' THEN 'PASS'
    ELSE 'INFO'
  END AS ts14_check,

  ts15,
  CASE
    WHEN case_id = 'ts15_offset_zone_mismatch' AND ts15 = '2024-05-20 08:15:30.123456' THEN 'PASS'
    WHEN case_id = 'ts15_without_brackets' AND ts15 IS NULL THEN 'PASS_EXPECTED_NULL'
    ELSE 'INFO'
  END AS ts15_check,

  ts16,
  CASE
    WHEN case_id = 'ts16_india' AND ts16 = '2024-05-20 10:00:00.123456' THEN 'PASS'
    ELSE 'INFO'
  END AS ts16_check,

  ts17,
  ts18,
  CASE
    WHEN case_id = 'ts18_midnight' AND ts18 = '00:00' THEN 'PASS'
    WHEN case_id = 'ts18_invalid_hour' AND ts18 IS NULL THEN 'PASS_EXPECTED_NULL'
    ELSE 'INFO'
  END AS ts18_check
FROM default.transformed_view
WHERE case_id IN (
  'datetime_min_boundary',
  'datetime_max_boundary',
  'datetime_string_out_of_range',
  'ts6_offset_0530',
  'ts6_offset_0300_textzone',
  'ts6_invalid_short_fraction',
  'ts13_invalid_no_offset',
  'ts14_day_shift_forward',
  'ts15_offset_zone_mismatch',
  'ts15_without_brackets',
  'ts16_india',
  'ts18_midnight',
  'ts18_invalid_hour'
)
ORDER BY case_id;

