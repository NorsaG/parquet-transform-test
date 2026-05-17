-- USE default;

-- TS04
WITH etalon AS (
  SELECT stack(5,
    'baseline_valid', '2024-05-20 10:15:30.123456',
    'datetime_min_boundary', '0001-01-01 00:00:00',
    'datetime_max_boundary', '9999-12-31 23:59:59.999999',
    'ts4_invalid_format', 'error',
    'nulls_all', NULL
  ) AS (case_id, etalon_value)
)
SELECT
  e.case_id,
  CAST(s.src_ts4 AS STRING) AS source_value,
  CAST(v.ts4 AS STRING) AS transformed_value,
  e.etalon_value,
  CASE
    WHEN e.etalon_value = 'error' THEN CASE WHEN v.ts4 IS NULL THEN 'SAME' ELSE 'NOT SAME' END
    WHEN CAST(v.ts4 AS STRING) <=> e.etalon_value THEN 'SAME'
    ELSE 'NOT SAME'
  END AS comparison_result
FROM etalon e
LEFT JOIN default.source_input s ON s.case_id = e.case_id
LEFT JOIN default.transformed_view v ON v.case_id = e.case_id
ORDER BY e.case_id;

-- TS05
WITH etalon AS (
  SELECT stack(5,
    'baseline_valid', '2024-05-20 10:15:30.123456',
    'datetime_min_boundary', '0001-01-01 00:00:00.000000',
    'datetime_max_boundary', '9999-12-31 23:59:59.999999',
    'ts5_invalid_nano_length', 'error',
    'nulls_all', NULL
  ) AS (case_id, etalon_value)
)
SELECT
  e.case_id,
  CAST(s.src_ts5 AS STRING) AS source_value,
  CAST(v.ts5 AS STRING) AS transformed_value,
  e.etalon_value,
  CASE
    WHEN e.etalon_value = 'error' THEN CASE WHEN v.ts5 IS NULL THEN 'SAME' ELSE 'NOT SAME' END
    WHEN CAST(v.ts5 AS STRING) <=> e.etalon_value THEN 'SAME'
    ELSE 'NOT SAME'
  END AS comparison_result
FROM etalon e
LEFT JOIN default.source_input s ON s.case_id = e.case_id
LEFT JOIN default.transformed_view v ON v.case_id = e.case_id
ORDER BY e.case_id;

-- TS06
WITH etalon AS (
  SELECT stack(5,
    'baseline_valid', '2026-04-13 13:42:43.271025+03',
    'ts6_offset_0530', '2026-04-13 13:42:43.271025+05:30',
    'ts6_offset_0300_textzone', '2026-04-13 13:42:43.271025+03',
    'ts6_invalid_short_fraction', 'error',
    'nulls_all', NULL
  ) AS (case_id, etalon_value)
)
SELECT
  e.case_id,
  CAST(s.src_ts6 AS STRING) AS source_value,
  CAST(v.ts6 AS STRING) AS transformed_value,
  e.etalon_value,
  CASE
    WHEN e.etalon_value = 'error' THEN CASE WHEN v.ts6 IS NULL THEN 'SAME' ELSE 'NOT SAME' END
    WHEN CAST(v.ts6 AS STRING) <=> e.etalon_value THEN 'SAME'
    ELSE 'NOT SAME'
  END AS comparison_result
FROM etalon e
LEFT JOIN default.source_input s ON s.case_id = e.case_id
LEFT JOIN default.transformed_view v ON v.case_id = e.case_id
ORDER BY e.case_id;

-- TS09
WITH etalon AS (
  SELECT stack(3,
    'baseline_valid', '9223372036854775806',
    'ts9_overflow', 'error',
    'nulls_all', NULL
  ) AS (case_id, etalon_value)
)
SELECT
  e.case_id,
  CAST(s.src_ts9 AS STRING) AS source_value,
  CAST(v.ts9 AS STRING) AS transformed_value,
  e.etalon_value,
  CASE
    WHEN e.etalon_value = 'error' THEN CASE WHEN v.ts9 IS NULL THEN 'SAME' ELSE 'NOT SAME' END
    WHEN CAST(v.ts9 AS STRING) <=> e.etalon_value THEN 'SAME'
    ELSE 'NOT SAME'
  END AS comparison_result
FROM etalon e
LEFT JOIN default.source_input s ON s.case_id = e.case_id
LEFT JOIN default.transformed_view v ON v.case_id = e.case_id
ORDER BY e.case_id;

-- TS10
WITH etalon AS (
  SELECT stack(3,
    'baseline_valid', '12.345600000000',
    'ts10_nan', 'error',
    'nulls_all', NULL
  ) AS (case_id, etalon_value)
)
SELECT
  e.case_id,
  CAST(s.src_ts10 AS STRING) AS source_value,
  CAST(v.ts10 AS STRING) AS transformed_value,
  e.etalon_value,
  CASE
    WHEN e.etalon_value = 'error' THEN CASE WHEN v.ts10 IS NULL THEN 'SAME' ELSE 'NOT SAME' END
    WHEN CAST(v.ts10 AS STRING) <=> e.etalon_value THEN 'SAME'
    ELSE 'NOT SAME'
  END AS comparison_result
FROM etalon e
LEFT JOIN default.source_input s ON s.case_id = e.case_id
LEFT JOIN default.transformed_view v ON v.case_id = e.case_id
ORDER BY e.case_id;

-- TS12
WITH etalon AS (
  SELECT stack(3,
    'baseline_valid', '456.789012000000',
    'ts12_positive_infinity', 'error',
    'nulls_all', NULL
  ) AS (case_id, etalon_value)
)
SELECT
  e.case_id,
  CAST(s.src_ts12 AS STRING) AS source_value,
  CAST(v.ts12 AS STRING) AS transformed_value,
  e.etalon_value,
  CASE
    WHEN e.etalon_value = 'error' THEN CASE WHEN v.ts12 IS NULL THEN 'SAME' ELSE 'NOT SAME' END
    WHEN CAST(v.ts12 AS STRING) <=> e.etalon_value THEN 'SAME'
    ELSE 'NOT SAME'
  END AS comparison_result
FROM etalon e
LEFT JOIN default.source_input s ON s.case_id = e.case_id
LEFT JOIN default.transformed_view v ON v.case_id = e.case_id
ORDER BY e.case_id;

-- TS13
WITH etalon AS (
  SELECT stack(3,
    'baseline_valid', '2024-05-20 10:15:30.123456',
    'ts13_invalid_no_offset', 'error',
    'nulls_all', NULL
  ) AS (case_id, etalon_value)
)
SELECT
  e.case_id,
  CAST(s.src_ts13 AS STRING) AS source_value,
  CAST(v.ts13 AS STRING) AS transformed_value,
  e.etalon_value,
  CASE
    WHEN e.etalon_value = 'error' THEN CASE WHEN v.ts13 IS NULL THEN 'SAME' ELSE 'NOT SAME' END
    WHEN CAST(v.ts13 AS STRING) <=> e.etalon_value THEN 'SAME'
    ELSE 'NOT SAME'
  END AS comparison_result
FROM etalon e
LEFT JOIN default.source_input s ON s.case_id = e.case_id
LEFT JOIN default.transformed_view v ON v.case_id = e.case_id
ORDER BY e.case_id;

-- TS14
WITH etalon AS (
  SELECT stack(3,
    'baseline_valid', '2024-05-20 10:15:30.123456',
    'ts14_day_shift_forward', '2024-05-21 04:00:00.000000',
    'nulls_all', NULL
  ) AS (case_id, etalon_value)
)
SELECT
  e.case_id,
  CAST(s.src_ts14 AS STRING) AS source_value,
  CAST(v.ts14 AS STRING) AS transformed_value,
  e.etalon_value,
  CASE
    WHEN e.etalon_value = 'error' THEN CASE WHEN v.ts14 IS NULL THEN 'SAME' ELSE 'NOT SAME' END
    WHEN CAST(v.ts14 AS STRING) <=> e.etalon_value THEN 'SAME'
    ELSE 'NOT SAME'
  END AS comparison_result
FROM etalon e
LEFT JOIN default.source_input s ON s.case_id = e.case_id
LEFT JOIN default.transformed_view v ON v.case_id = e.case_id
ORDER BY e.case_id;

-- TS15
WITH etalon AS (
  SELECT stack(4,
    'baseline_valid', '2024-05-20 13:15:30.123456',
    'ts15_offset_zone_mismatch', '2024-05-20 08:15:30.123456',
    'ts15_without_brackets', 'error',
    'nulls_all', NULL
  ) AS (case_id, etalon_value)
)
SELECT
  e.case_id,
  CAST(s.src_ts15 AS STRING) AS source_value,
  CAST(v.ts15 AS STRING) AS transformed_value,
  e.etalon_value,
  CASE
    WHEN e.etalon_value = 'error' THEN CASE WHEN v.ts15 IS NULL THEN 'SAME' ELSE 'NOT SAME' END
    WHEN CAST(v.ts15 AS STRING) <=> e.etalon_value THEN 'SAME'
    ELSE 'NOT SAME'
  END AS comparison_result
FROM etalon e
LEFT JOIN default.source_input s ON s.case_id = e.case_id
LEFT JOIN default.transformed_view v ON v.case_id = e.case_id
ORDER BY e.case_id;

-- TS16
WITH etalon AS (
  SELECT stack(3,
    'baseline_valid', '2024-05-20 10:00:00.123456',
    'ts16_india', '2024-05-20 10:00:00.123456',
    'nulls_all', NULL
  ) AS (case_id, etalon_value)
)
SELECT
  e.case_id,
  CAST(s.src_ts16 AS STRING) AS source_value,
  CAST(v.ts16 AS STRING) AS transformed_value,
  e.etalon_value,
  CASE
    WHEN e.etalon_value = 'error' THEN CASE WHEN v.ts16 IS NULL THEN 'SAME' ELSE 'NOT SAME' END
    WHEN CAST(v.ts16 AS STRING) <=> e.etalon_value THEN 'SAME'
    ELSE 'NOT SAME'
  END AS comparison_result
FROM etalon e
LEFT JOIN default.source_input s ON s.case_id = e.case_id
LEFT JOIN default.transformed_view v ON v.case_id = e.case_id
ORDER BY e.case_id;

-- TS18
WITH etalon AS (
  SELECT stack(4,
    'baseline_valid', '13:42:43.123456789',
    'ts18_midnight', '00:00',
    'ts18_invalid_hour', 'error',
    'nulls_all', NULL
  ) AS (case_id, etalon_value)
)
SELECT
  e.case_id,
  CAST(s.src_ts18 AS STRING) AS source_value,
  CAST(v.ts18 AS STRING) AS transformed_value,
  e.etalon_value,
  CASE
    WHEN e.etalon_value = 'error' THEN CASE WHEN v.ts18 IS NULL THEN 'SAME' ELSE 'NOT SAME' END
    WHEN CAST(v.ts18 AS STRING) <=> e.etalon_value THEN 'SAME'
    ELSE 'NOT SAME'
  END AS comparison_result
FROM etalon e
LEFT JOIN default.source_input s ON s.case_id = e.case_id
LEFT JOIN default.transformed_view v ON v.case_id = e.case_id
ORDER BY e.case_id;

-- TS19
WITH etalon AS (
  SELECT stack(3,
    'baseline_valid', 'part one text:part two text',
    'ts19_without_colon', 'error',
    'nulls_all', NULL
  ) AS (case_id, etalon_value)
)
SELECT
  e.case_id,
  CAST(s.src_ts19 AS STRING) AS source_value,
  CAST(v.ts19 AS STRING) AS transformed_value,
  e.etalon_value,
  CASE
    WHEN e.etalon_value = 'error' THEN CASE WHEN v.ts19 IS NULL THEN 'SAME' ELSE 'NOT SAME' END
    WHEN CAST(v.ts19 AS STRING) <=> e.etalon_value THEN 'SAME'
    ELSE 'NOT SAME'
  END AS comparison_result
FROM etalon e
LEFT JOIN default.source_input s ON s.case_id = e.case_id
LEFT JOIN default.transformed_view v ON v.case_id = e.case_id
ORDER BY e.case_id;

-- TS20 (regex etalon because timestamp part is dynamic)
WITH etalon AS (
  SELECT stack(4,
    'baseline_valid', 'k1:TypeA\\|[0-9]+:I',
    'ts20_empty_array', '',
    'ts20_invalid_json', 'error',
    'nulls_all', NULL
  ) AS (case_id, etalon_value)
)
SELECT
  e.case_id,
  CAST(s.src_ts20 AS STRING) AS source_value,
  CAST(v.ts20 AS STRING) AS transformed_value,
  e.etalon_value,
  CASE
    WHEN e.etalon_value = 'error' THEN CASE WHEN v.ts20 IS NULL THEN 'SAME' ELSE 'NOT SAME' END
    WHEN e.etalon_value IS NULL THEN CASE WHEN v.ts20 IS NULL THEN 'SAME' ELSE 'NOT SAME' END
    WHEN CAST(v.ts20 AS STRING) RLIKE e.etalon_value THEN 'SAME'
    ELSE 'NOT SAME'
  END AS comparison_result
FROM etalon e
LEFT JOIN default.source_input s ON s.case_id = e.case_id
LEFT JOIN default.transformed_view v ON v.case_id = e.case_id
ORDER BY e.case_id;

-- TS21
WITH etalon AS (
  SELECT stack(4,
    'baseline_valid', '123|12345|null',
    'ts21_empty_array', '',
    'ts21_only_nulls', 'null|null',
    'nulls_all', NULL
  ) AS (case_id, etalon_value)
)
SELECT
  e.case_id,
  CAST(s.src_ts21 AS STRING) AS source_value,
  CAST(v.ts21 AS STRING) AS transformed_value,
  e.etalon_value,
  CASE
    WHEN e.etalon_value = 'error' THEN CASE WHEN v.ts21 IS NULL THEN 'SAME' ELSE 'NOT SAME' END
    WHEN CAST(v.ts21 AS STRING) <=> e.etalon_value THEN 'SAME'
    ELSE 'NOT SAME'
  END AS comparison_result
FROM etalon e
LEFT JOIN default.source_input s ON s.case_id = e.case_id
LEFT JOIN default.transformed_view v ON v.case_id = e.case_id
ORDER BY e.case_id;


