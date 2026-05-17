-- USE default;

-- TS04: expected rows >= 5
WITH cases AS (
  SELECT stack(5,
    'ts04_case_01',
    'ts04_case_02',
    'ts04_case_03',
    'ts04_case_04',
    'ts04_case_05'
  ) AS case_id
)
SELECT c.case_id, s.src_ts4 AS source_value, v.ts4 AS transformed_value
FROM cases c
LEFT JOIN default.source_input s ON s.case_id = c.case_id
LEFT JOIN default.transformed_view v ON v.case_id = c.case_id
ORDER BY c.case_id;

-- TS05: expected rows >= 19
WITH cases AS (
  SELECT stack(19,
    'ts05_case_01','ts05_case_02','ts05_case_03','ts05_case_04','ts05_case_05',
    'ts05_case_06','ts05_case_07','ts05_case_08','ts05_case_09','ts05_case_10',
    'ts05_case_11','ts05_case_12','ts05_case_13','ts05_case_14','ts05_case_15',
    'ts05_case_16','ts05_case_17','ts05_case_18','ts05_case_19'
  ) AS case_id
)
SELECT c.case_id, s.src_ts5 AS source_value, v.ts5 AS transformed_value
FROM cases c
LEFT JOIN default.source_input s ON s.case_id = c.case_id
LEFT JOIN default.transformed_view v ON v.case_id = c.case_id
ORDER BY c.case_id;

-- TS06: expected rows >= 20
WITH cases AS (
  SELECT stack(20,
    'ts06_case_01','ts06_case_02','ts06_case_03','ts06_case_04','ts06_case_05',
    'ts06_case_06','ts06_case_07','ts06_case_08','ts06_case_09','ts06_case_10',
    'ts06_case_11','ts06_case_12','ts06_case_13','ts06_case_14','ts06_case_15',
    'ts06_case_16','ts06_case_17','ts06_case_18','ts06_case_19','ts06_case_20'
  ) AS case_id
)
SELECT c.case_id, s.src_ts6 AS source_value, v.ts6 AS transformed_value
FROM cases c
LEFT JOIN default.source_input s ON s.case_id = c.case_id
LEFT JOIN default.transformed_view v ON v.case_id = c.case_id
ORDER BY c.case_id;

-- TS09: expected rows >= 20
WITH cases AS (
  SELECT stack(20,
    'ts09_case_01','ts09_case_02','ts09_case_03','ts09_case_04','ts09_case_05',
    'ts09_case_06','ts09_case_07','ts09_case_08','ts09_case_09','ts09_case_10',
    'ts09_case_11','ts09_case_12','ts09_case_13','ts09_case_14','ts09_case_15',
    'ts09_case_16','ts09_case_17','ts09_case_18','ts09_case_19','ts09_case_20'
  ) AS case_id
)
SELECT c.case_id, s.src_ts9 AS source_value, v.ts9 AS transformed_value
FROM cases c
LEFT JOIN default.source_input s ON s.case_id = c.case_id
LEFT JOIN default.transformed_view v ON v.case_id = c.case_id
ORDER BY c.case_id;

-- TS10: expected rows >= 18
WITH cases AS (
  SELECT stack(18,
    'ts10_case_01','ts10_case_02','ts10_case_03','ts10_case_04','ts10_case_05','ts10_case_06',
    'ts10_case_07','ts10_case_08','ts10_case_09','ts10_case_10','ts10_case_11','ts10_case_12',
    'ts10_case_13','ts10_case_14','ts10_case_15','ts10_case_16','ts10_case_17','ts10_case_18'
  ) AS case_id
)
SELECT c.case_id, CAST(s.src_ts10 AS STRING) AS source_value, CAST(v.ts10 AS STRING) AS transformed_value
FROM cases c
LEFT JOIN default.source_input s ON s.case_id = c.case_id
LEFT JOIN default.transformed_view v ON v.case_id = c.case_id
ORDER BY c.case_id;

-- TS12: expected rows >= 14
WITH cases AS (
  SELECT stack(14,
    'ts12_case_01','ts12_case_02','ts12_case_03','ts12_case_04','ts12_case_05','ts12_case_06','ts12_case_07',
    'ts12_case_08','ts12_case_09','ts12_case_10','ts12_case_11','ts12_case_12','ts12_case_13','ts12_case_14'
  ) AS case_id
)
SELECT c.case_id, CAST(s.src_ts12 AS STRING) AS source_value, CAST(v.ts12 AS STRING) AS transformed_value
FROM cases c
LEFT JOIN default.source_input s ON s.case_id = c.case_id
LEFT JOIN default.transformed_view v ON v.case_id = c.case_id
ORDER BY c.case_id;

-- TS13: expected rows >= 19
WITH cases AS (
  SELECT stack(19,
    'ts13_case_01','ts13_case_02','ts13_case_03','ts13_case_04','ts13_case_05',
    'ts13_case_06','ts13_case_07','ts13_case_08','ts13_case_09','ts13_case_10',
    'ts13_case_11','ts13_case_12','ts13_case_13','ts13_case_14','ts13_case_15',
    'ts13_case_16','ts13_case_17','ts13_case_18','ts13_case_19'
  ) AS case_id
)
SELECT c.case_id, s.src_ts13 AS source_value, v.ts13 AS transformed_value
FROM cases c
LEFT JOIN default.source_input s ON s.case_id = c.case_id
LEFT JOIN default.transformed_view v ON v.case_id = c.case_id
ORDER BY c.case_id;

-- TS14: expected rows >= 22
WITH cases AS (
  SELECT stack(22,
    'ts14_case_01','ts14_case_02','ts14_case_03','ts14_case_04','ts14_case_05','ts14_case_06',
    'ts14_case_07','ts14_case_08','ts14_case_09','ts14_case_10','ts14_case_11','ts14_case_12',
    'ts14_case_13','ts14_case_14','ts14_case_15','ts14_case_16','ts14_case_17','ts14_case_18',
    'ts14_case_19','ts14_case_20','ts14_case_21','ts14_case_22'
  ) AS case_id
)
SELECT c.case_id, s.src_ts14 AS source_value, v.ts14 AS transformed_value
FROM cases c
LEFT JOIN default.source_input s ON s.case_id = c.case_id
LEFT JOIN default.transformed_view v ON v.case_id = c.case_id
ORDER BY c.case_id;

-- TS15: expected rows >= 23
WITH cases AS (
  SELECT stack(23,
    'ts15_case_01','ts15_case_02','ts15_case_03','ts15_case_04','ts15_case_05','ts15_case_06',
    'ts15_case_07','ts15_case_08','ts15_case_09','ts15_case_10','ts15_case_11','ts15_case_12',
    'ts15_case_13','ts15_case_14','ts15_case_15','ts15_case_16','ts15_case_17','ts15_case_18',
    'ts15_case_19','ts15_case_20','ts15_case_21','ts15_case_22','ts15_case_23'
  ) AS case_id
)
SELECT c.case_id, s.src_ts15 AS source_value, v.ts15 AS transformed_value
FROM cases c
LEFT JOIN default.source_input s ON s.case_id = c.case_id
LEFT JOIN default.transformed_view v ON v.case_id = c.case_id
ORDER BY c.case_id;

-- TS16: expected rows >= 10
WITH cases AS (
  SELECT stack(10,
    'ts16_case_01','ts16_case_02','ts16_case_03','ts16_case_04','ts16_case_05',
    'ts16_case_06','ts16_case_07','ts16_case_08','ts16_case_09','ts16_case_10'
  ) AS case_id
)
SELECT c.case_id, s.src_ts16 AS source_value, v.ts16 AS transformed_value
FROM cases c
LEFT JOIN default.source_input s ON s.case_id = c.case_id
LEFT JOIN default.transformed_view v ON v.case_id = c.case_id
ORDER BY c.case_id;

-- TS18: expected rows >= 11
WITH cases AS (
  SELECT stack(11,
    'ts18_case_01','ts18_case_02','ts18_case_03','ts18_case_04','ts18_case_05',
    'ts18_case_06','ts18_case_07','ts18_case_08','ts18_case_09','ts18_case_10','ts18_case_11'
  ) AS case_id
)
SELECT c.case_id, s.src_ts18 AS source_value, v.ts18 AS transformed_value
FROM cases c
LEFT JOIN default.source_input s ON s.case_id = c.case_id
LEFT JOIN default.transformed_view v ON v.case_id = c.case_id
ORDER BY c.case_id;

-- TS19: expected rows >= 10
WITH cases AS (
  SELECT stack(10,
    'ts19_case_01','ts19_case_02','ts19_case_03','ts19_case_04','ts19_case_05',
    'ts19_case_06','ts19_case_07','ts19_case_08','ts19_case_09','ts19_case_10'
  ) AS case_id
)
SELECT c.case_id, s.src_ts19 AS source_value, v.ts19 AS transformed_value
FROM cases c
LEFT JOIN default.source_input s ON s.case_id = c.case_id
LEFT JOIN default.transformed_view v ON v.case_id = c.case_id
ORDER BY c.case_id;

-- TS20: expected rows >= 14
WITH cases AS (
  SELECT stack(14,
    'ts20_case_01','ts20_case_02','ts20_case_03','ts20_case_04','ts20_case_05','ts20_case_06','ts20_case_07',
    'ts20_case_08','ts20_case_09','ts20_case_10','ts20_case_11','ts20_case_12','ts20_case_13','ts20_case_14'
  ) AS case_id
)
SELECT c.case_id, s.src_ts20 AS source_value, v.ts20 AS transformed_value
FROM cases c
LEFT JOIN default.source_input s ON s.case_id = c.case_id
LEFT JOIN default.transformed_view v ON v.case_id = c.case_id
ORDER BY c.case_id;

-- TS21: expected rows >= 6
WITH cases AS (
  SELECT stack(6,
    'ts21_case_01','ts21_case_02','ts21_case_03','ts21_case_04','ts21_case_05','ts21_case_06'
  ) AS case_id
)
SELECT c.case_id, s.src_ts21 AS source_value, v.ts21 AS transformed_value
FROM cases c
LEFT JOIN default.source_input s ON s.case_id = c.case_id
LEFT JOIN default.transformed_view v ON v.case_id = c.case_id
ORDER BY c.case_id;

