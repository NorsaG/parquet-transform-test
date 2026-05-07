USE transform_demo;

-- Diagnostic block: shows what case ids are currently available.
SELECT ts19 AS case_id, COUNT(*) AS cnt
FROM transformed_view
GROUP BY ts19
ORDER BY case_id;

SELECT
  ts19 AS case_id,
  ts1,
  CASE
    WHEN ts1 >= TIMESTAMP '0001-01-01 00:00:00' AND ts1 <= TIMESTAMP '9999-12-31 23:59:59'
      THEN 'IN_RANGE'
    ELSE 'OUT_OF_RANGE_OR_NULL'
  END AS ts1_range_check,
  ts4,
  CASE
    WHEN ts4 >= TIMESTAMP '0001-01-01 00:00:00' AND ts4 <= TIMESTAMP '9999-12-31 23:59:59'
      THEN 'IN_RANGE'
    ELSE 'OUT_OF_RANGE_OR_NULL'
  END AS ts4_range_check,
  ts5,
  ts6,
  ts13,
  ts14,
  ts15,
  ts16,
  ts17,
  ts18
FROM transformed_view
WHERE LOWER(ts19) RLIKE '^(case[_\.]boundary:(min|max)|case[_\.]outofrange:strings)$'
ORDER BY case_id;

