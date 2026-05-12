import java.sql.Date

spark.sparkContext.setLogLevel("WARN")
import spark.implicits._

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")

val parquetBasePath = sys.env.getOrElse("PARQUET_PATH", "/data/parquet/input")
val parquetPath = s"$parquetBasePath/run_${System.currentTimeMillis()}"

case class SourceRecord(
  case_id: String,
  src_ts1: Date,
  src_ts2: java.lang.Integer,
  src_ts3: String,
  src_ts4: String,
  src_ts5: String,
  src_ts6: String,
  src_ts7: String,
  src_ts8: String,
  src_ts9: String,
  src_ts10: java.lang.Float,
  src_ts11: java.lang.Float,
  src_ts12: java.lang.Double,
  src_ts13: String,
  src_ts14: String,
  src_ts15: String,
  src_ts16: String,
  src_ts17: Date,
  src_ts18: String,
  src_ts19: String,
  src_ts20: String,
  src_ts21: String
)

val base = SourceRecord(
  case_id = "baseline_valid",
  src_ts1 = Date.valueOf("2024-05-20"),
  src_ts2 = Int.box(123),
  src_ts3 = "12345.678901234567",
  src_ts4 = "2024-05-20T10:15:30.123456789",
  src_ts5 = "2024-05-20T10:15:30.123456789",
  src_ts6 = "2026-04-13T13:42:43.271025800+03:00[Europe/Moscow]",
  src_ts7 = "999.99",
  src_ts8 = "23:59:59.98765432",
  src_ts9 = "9223372036854775806",
  src_ts10 = Float.box(12.3456f),
  src_ts11 = Float.box(7.5f),
  src_ts12 = Double.box(456.789012d),
  src_ts13 = "2024-05-20T10:15:30.123456789+03:00",
  src_ts14 = "2024-05-20T13:15:30.123456789+03:00",
  src_ts15 = "2024-05-20T13:15:30.123456789+03:00[Europe/Moscow]",
  src_ts16 = "2024-05-20T13:00:00.123456789+03:00[Europe/Moscow]",
  src_ts17 = Date.valueOf("2024-05-20"),
  src_ts18 = "13:42:43.123456789",
  src_ts19 = "part.one.text:part.two.text",
  src_ts20 = "[{\"key\":\"k1\",\"type\":\"TypeA\"}]",
  src_ts21 = "[\"123\",\"12345\",null]"
)

val sourceRows = Seq(
  base,
  base.copy(case_id = "datetime_min_boundary", src_ts1 = Date.valueOf("0001-01-01"), src_ts4 = "0001-01-01T00:00:00.000000000", src_ts5 = "0001-01-01T00:00:00.000000000", src_ts13 = "0001-01-01T00:00:00.000000000+00:00", src_ts14 = "0001-01-01T00:00:00.000000000+00:00", src_ts15 = "0001-01-01T00:00:00.000000000Z[UTC]", src_ts16 = "0001-01-01T00:00:00.000000000Z[UTC]", src_ts17 = Date.valueOf("0001-01-01")),
  base.copy(case_id = "datetime_max_boundary", src_ts1 = Date.valueOf("9999-12-31"), src_ts4 = "9999-12-31T23:59:59.999999999", src_ts5 = "9999-12-31T23:59:59.999999999", src_ts13 = "9999-12-31T23:59:59.999999999+00:00", src_ts14 = "9999-12-31T23:59:59.999999999+00:00", src_ts15 = "9999-12-31T23:59:59.999999999Z[UTC]", src_ts16 = "9999-12-31T23:59:59.999999999Z[UTC]", src_ts17 = Date.valueOf("9999-12-31")),
  base.copy(case_id = "datetime_string_out_of_range", src_ts5 = "0000-01-01T00:00:00.000000000", src_ts13 = "10000-01-01T00:00:00.000000000+00:00", src_ts14 = "0000-01-01T00:00:00.000000000+00:00", src_ts15 = "-0001-12-31T23:59:59.000000000+00:00[UTC]", src_ts16 = "10000-01-01T00:00:00.000000000Z[UTC]"),
  base.copy(case_id = "ts4_invalid_format", src_ts4 = "2024-05-20 10:15:30.123456789"),
  base.copy(case_id = "ts5_invalid_nano_length", src_ts5 = "2024-05-20T10:15:30.123"),
  base.copy(case_id = "ts6_offset_0530", src_ts6 = "2026-04-13T13:42:43.271025800+05:30"),
  base.copy(case_id = "ts6_offset_0300_textzone", src_ts6 = "2026-04-13T13:42:43.271025800+03:00Europe/Moscow"),
  base.copy(case_id = "ts6_invalid_short_fraction", src_ts6 = "2026-04-13T13:42:43.271025+03:00"),
  base.copy(case_id = "ts9_overflow", src_ts9 = "9223372036854775808"),
  base.copy(case_id = "ts10_nan", src_ts10 = Float.box(Float.NaN)),
  base.copy(case_id = "ts12_positive_infinity", src_ts12 = Double.box(Double.PositiveInfinity)),
  base.copy(case_id = "ts13_invalid_no_offset", src_ts13 = "2024-05-20T10:15:30.123456789"),
  base.copy(case_id = "ts14_day_shift_forward", src_ts14 = "2024-05-20T23:00:00.000000000-05:00"),
  base.copy(case_id = "ts15_offset_zone_mismatch", src_ts15 = "2024-05-20T10:15:30.123456789+05:00[Europe/Moscow]"),
  base.copy(case_id = "ts15_without_brackets", src_ts15 = "2024-05-20T10:15:30.123456789+03:00Europe/Moscow"),
  base.copy(case_id = "ts16_india", src_ts16 = "2024-05-20T15:30:00.123456789+05:30[Asia/Kolkata]"),
  base.copy(case_id = "ts18_midnight", src_ts18 = "00:00:00.000000000"),
  base.copy(case_id = "ts18_invalid_hour", src_ts18 = "25:00:00.000000000"),
  base.copy(case_id = "ts19_without_colon", src_ts19 = "part.one.no.colon"),
  base.copy(case_id = "ts20_empty_array", src_ts20 = "[]"),
  base.copy(case_id = "ts20_invalid_json", src_ts20 = "[{\"key\":\"k1\""),
  base.copy(case_id = "ts21_empty_array", src_ts21 = "[]"),
  base.copy(case_id = "ts21_only_nulls", src_ts21 = "[null,null]"),
  base.copy(case_id = "nulls_all", src_ts1 = null, src_ts2 = null, src_ts3 = null, src_ts4 = null, src_ts5 = null, src_ts6 = null, src_ts7 = null, src_ts8 = null, src_ts9 = null, src_ts10 = null, src_ts11 = null, src_ts12 = null, src_ts13 = null, src_ts14 = null, src_ts15 = null, src_ts16 = null, src_ts17 = null, src_ts18 = null, src_ts19 = null, src_ts20 = null, src_ts21 = null)
)

val sourceDf = spark.createDataset(sourceRows).toDF()
sourceDf.write.mode("overwrite").parquet(parquetPath)

spark.sql("CREATE DATABASE IF NOT EXISTS transform_demo")
spark.sql("DROP TABLE IF EXISTS transform_demo.source_input")
spark.sql(s"""
  CREATE EXTERNAL TABLE transform_demo.source_input (
    case_id STRING,
    src_ts1 DATE,
    src_ts2 INT,
    src_ts3 STRING,
    src_ts4 STRING,
    src_ts5 STRING,
    src_ts6 STRING,
    src_ts7 STRING,
    src_ts8 STRING,
    src_ts9 STRING,
    src_ts10 FLOAT,
    src_ts11 FLOAT,
    src_ts12 DOUBLE,
    src_ts13 STRING,
    src_ts14 STRING,
    src_ts15 STRING,
    src_ts16 STRING,
    src_ts17 DATE,
    src_ts18 STRING,
    src_ts19 STRING,
    src_ts20 STRING,
    src_ts21 STRING
  )
  STORED AS PARQUET
  LOCATION '$parquetPath'
""")

spark.sql("DROP VIEW IF EXISTS transform_demo.transformed_view")
spark.sql("""
  CREATE VIEW transform_demo.transformed_view AS
  SELECT
    case_id,
    CAST(src_ts1 AS TIMESTAMP) AS ts1,
    CAST(src_ts2 AS SMALLINT) AS ts2,
    CAST(src_ts3 AS DECIMAL(38,12)) AS ts3,

    -- TS4: strict yyyy-MM-ddTHH:mm:ss.SSSSSSSSS -> timestamp (microseconds)
    CASE
      WHEN src_ts4 IS NULL OR TRIM(src_ts4) = '' THEN NULL
      WHEN TRIM(src_ts4) RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}$'
        THEN TO_TIMESTAMP(SUBSTR(TRIM(src_ts4), 1, 26), 'yyyy-MM-dd\'T\'HH:mm:ss.SSSSSS')
      ELSE NULL
    END AS ts4,

    -- TS5: strict yyyy-MM-ddTHH:mm:ss.SSSSSSSSS -> yyyy-MM-dd HH:mm:ss.SSSSSS
    CASE
      WHEN src_ts5 IS NULL OR TRIM(src_ts5) = '' THEN NULL
      WHEN TRIM(src_ts5) RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}$'
           AND TO_TIMESTAMP(CONCAT(SUBSTR(TRIM(src_ts5),1,10), ' ', SUBSTR(TRIM(src_ts5),12,8)), 'yyyy-MM-dd HH:mm:ss') IS NOT NULL
        THEN CONCAT(SUBSTR(TRIM(src_ts5), 1, 10), ' ', SUBSTR(TRIM(src_ts5), 12, 8), '.', SUBSTR(TRIM(src_ts5), 21, 6))
      ELSE NULL
    END AS ts5,

    -- TS6
    CASE
      WHEN src_ts6 IS NULL OR TRIM(src_ts6) = '' THEN NULL
      WHEN TRIM(src_ts6) RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}.*$'
        THEN CONCAT(
          REGEXP_REPLACE(REGEXP_EXTRACT(TRIM(src_ts6), '^(\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2})', 1), 'T', ' '),
          '.',
          SUBSTR(REGEXP_EXTRACT(TRIM(src_ts6), '^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}\\.([0-9]{9})', 1), 1, 6),
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                CASE
                  WHEN REGEXP_EXTRACT(TRIM(src_ts6), '(Z|[+-]\\d{2}:?\\d{2})', 1) = ''
                    THEN CASE WHEN TRIM(src_ts6) LIKE '%Europe/Moscow%' THEN '+03' ELSE '+00' END
                  ELSE REGEXP_REPLACE(REGEXP_EXTRACT(TRIM(src_ts6), '(Z|[+-]\\d{2}:?\\d{2})', 1), '^Z$', '+00')
                END,
                '^([+-]\\d{2})(\\d{2})$', '$1:$2'
              ),
              '^([+-]\\d{2}):00$', '$1'
            ),
            '^([+-]\\d{2})00$', '$1'
          )
        )
      ELSE NULL
    END AS ts6,

    CAST(src_ts7 AS DECIMAL(19,2)) AS ts7,

    -- TS8: HH:mm:ss.SSSSSSSS -> HH:mm:ss.SSSSSS
    CASE
      WHEN src_ts8 IS NULL OR TRIM(src_ts8) = '' THEN NULL
      WHEN TRIM(src_ts8) RLIKE '^(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]\.[0-9]{8}$'
        THEN DATE_FORMAT(TO_TIMESTAMP(TRIM(src_ts8), 'HH:mm:ss.SSSSSSSS'), 'HH:mm:ss.SSSSSS')
      ELSE NULL
    END AS ts8,

    CAST(src_ts9 AS BIGINT) AS ts9,

    -- TS10/TS12: cast keeps null for NaN/Infinity in Spark SQL
    CAST(CAST(src_ts10 AS STRING) AS DECIMAL(38,12)) AS ts10,
    CAST(src_ts11 AS DOUBLE) AS ts11,
    CAST(src_ts12 AS DECIMAL(38,12)) AS ts12,

    -- TS13: strict offset datetime to local string micros
    CASE
      WHEN src_ts13 IS NULL OR TRIM(src_ts13) = '' THEN NULL
      WHEN TRIM(src_ts13) RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}(Z|[+-][0-9]{2}:?[0-9]{2})$'
           AND TO_TIMESTAMP(CONCAT(SUBSTR(TRIM(src_ts13),1,10), ' ', SUBSTR(TRIM(src_ts13),12,8)), 'yyyy-MM-dd HH:mm:ss') IS NOT NULL
        THEN CONCAT(SUBSTR(TRIM(src_ts13),1,10), ' ', SUBSTR(TRIM(src_ts13),12,8), '.', SUBSTR(TRIM(src_ts13),21,6))
      ELSE NULL
    END AS ts13,

    -- TS14: strict offset datetime to UTC string micros
    CASE
      WHEN src_ts14 IS NULL OR TRIM(src_ts14) = '' THEN NULL
      WHEN TRIM(src_ts14) RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}(Z|[+-][0-9]{2}:?[0-9]{2})$'
           AND TO_TIMESTAMP(CONCAT(SUBSTR(TRIM(src_ts14),1,10), ' ', SUBSTR(TRIM(src_ts14),12,8), '.', SUBSTR(TRIM(src_ts14),21,6)), 'yyyy-MM-dd HH:mm:ss.SSSSSS') IS NOT NULL
        THEN DATE_FORMAT(
          TO_UTC_TIMESTAMP(
            TO_TIMESTAMP(CONCAT(SUBSTR(TRIM(src_ts14),1,10), ' ', SUBSTR(TRIM(src_ts14),12,8), '.', SUBSTR(TRIM(src_ts14),21,6)), 'yyyy-MM-dd HH:mm:ss.SSSSSS'),
            CASE
              WHEN REGEXP_EXTRACT(TRIM(src_ts14), '(Z|[+-][0-9]{2}:?[0-9]{2})$', 1) = 'Z' THEN '+00:00'
              WHEN REGEXP_EXTRACT(TRIM(src_ts14), '(Z|[+-][0-9]{2}:?[0-9]{2})$', 1) RLIKE '^[+-][0-9]{4}$'
                THEN REGEXP_REPLACE(REGEXP_EXTRACT(TRIM(src_ts14), '(Z|[+-][0-9]{2}:?[0-9]{2})$', 1), '^([+-][0-9]{2})([0-9]{2})$', '$1:$2')
              ELSE REGEXP_EXTRACT(TRIM(src_ts14), '(Z|[+-][0-9]{2}:?[0-9]{2})$', 1)
            END
          ),
          'yyyy-MM-dd HH:mm:ss.SSSSSS'
        )
      ELSE NULL
    END AS ts14,

    -- TS15: zoned datetime as-is zone, formatted with micros
    CASE
      WHEN src_ts15 IS NULL OR TRIM(src_ts15) = '' THEN NULL
      WHEN TRIM(src_ts15) RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}Z$'
        THEN CONCAT(SUBSTR(TRIM(src_ts15),1,10), ' ', SUBSTR(TRIM(src_ts15),12,8), '.', SUBSTR(TRIM(src_ts15),21,6))
      WHEN TRIM(src_ts15) LIKE '%[%' AND TRIM(src_ts15) LIKE '%]%'
        THEN DATE_FORMAT(
          FROM_UTC_TIMESTAMP(
            TO_UTC_TIMESTAMP(
              TO_TIMESTAMP(CONCAT(SUBSTR(TRIM(src_ts15),1,10), ' ', SUBSTR(TRIM(src_ts15),12,8), '.', SUBSTR(TRIM(src_ts15),21,6)), 'yyyy-MM-dd HH:mm:ss.SSSSSS'),
              CASE
                WHEN REGEXP_EXTRACT(TRIM(src_ts15), '(Z|[+-][0-9]{2}:[0-9]{2})', 1) = 'Z' THEN '+00:00'
                ELSE REGEXP_EXTRACT(TRIM(src_ts15), '(Z|[+-][0-9]{2}:[0-9]{2})', 1)
              END
            ),
            REGEXP_EXTRACT(TRIM(src_ts15), '\\[(.+)\\]$', 1)
          ),
          'yyyy-MM-dd HH:mm:ss.SSSSSS'
        )
      ELSE NULL
    END AS ts15,

    -- TS16: zoned datetime to UTC with micros
    CASE
      WHEN src_ts16 IS NULL OR TRIM(src_ts16) = '' THEN NULL
      WHEN TRIM(src_ts16) LIKE '%[%' AND TRIM(src_ts16) LIKE '%]%'
        THEN DATE_FORMAT(
          TO_UTC_TIMESTAMP(
            TO_TIMESTAMP(CONCAT(SUBSTR(TRIM(src_ts16),1,10), ' ', SUBSTR(TRIM(src_ts16),12,8), '.', SUBSTR(TRIM(src_ts16),21,6)), 'yyyy-MM-dd HH:mm:ss.SSSSSS'),
            CASE
              WHEN REGEXP_EXTRACT(TRIM(src_ts16), '(Z|[+-][0-9]{2}:[0-9]{2})', 1) = 'Z' THEN '+00:00'
              ELSE REGEXP_EXTRACT(TRIM(src_ts16), '(Z|[+-][0-9]{2}:[0-9]{2})', 1)
            END
          ),
          'yyyy-MM-dd HH:mm:ss.SSSSSS'
        )
      ELSE NULL
    END AS ts16,

    CONCAT(CAST(src_ts17 AS STRING), ' 00:00:00.000000') AS ts17,

    -- TS18: LocalTime.toString()-like output
    CASE
      WHEN src_ts18 IS NULL OR TRIM(src_ts18) = '' THEN NULL
      WHEN TRIM(src_ts18) RLIKE '^(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]\.[0-9]{9}$' THEN
        CASE
          WHEN SUBSTR(TRIM(src_ts18), 10, 9) = '000000000' AND SUBSTR(TRIM(src_ts18), 1, 8) = '00:00:00' THEN '00:00'
          WHEN SUBSTR(TRIM(src_ts18), 10, 9) = '000000000' THEN SUBSTR(TRIM(src_ts18), 1, 8)
          ELSE CONCAT(SUBSTR(TRIM(src_ts18), 1, 8), '.', REGEXP_REPLACE(SUBSTR(TRIM(src_ts18), 10, 9), '0+$', ''))
        END
      ELSE NULL
    END AS ts18,

    -- TS19
    CASE
      WHEN src_ts19 IS NULL THEN NULL
      WHEN TRIM(src_ts19) = '' THEN ''
      WHEN LENGTH(src_ts19) - LENGTH(REGEXP_REPLACE(src_ts19, ':', '')) = 1
        THEN CONCAT(REGEXP_REPLACE(SPLIT(src_ts19, ':')[0], '\\.', ' '), ':', SPLIT(src_ts19, ':')[1])
      ELSE NULL
    END AS ts19,

    -- TS20 (Hive-compatible approximation without Spark lambda/json array transform)
    CASE
      WHEN src_ts20 IS NULL THEN NULL
      WHEN TRIM(src_ts20) = '' THEN ''
      WHEN TRIM(src_ts20) = '[]' THEN ''
      WHEN TRIM(src_ts20) LIKE '[%' AND TRIM(src_ts20) LIKE '%]' AND INSTR(src_ts20, '"key"') > 0 THEN
        CASE
          WHEN COALESCE(REGEXP_EXTRACT(src_ts20, '"type"\s*:\s*"([^"]*)"', 1), '') <> ''
            THEN CONCAT(
              COALESCE(REGEXP_EXTRACT(src_ts20, '"key"\s*:\s*"([^"]*)"', 1), ''),
              ':',
              REGEXP_EXTRACT(src_ts20, '"type"\s*:\s*"([^"]*)"', 1),
              '|',
              CAST(CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) AS BIGINT) * 1000 AS STRING),
              ':I'
            )
          ELSE CONCAT(
              COALESCE(REGEXP_EXTRACT(src_ts20, '"key"\s*:\s*"([^"]*)"', 1), ''),
              ':',
              CAST(CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) AS BIGINT) * 1000 AS STRING),
              ':I'
            )
        END
      ELSE NULL
    END AS ts20,

    -- TS21 (Hive-compatible without Spark lambda/json transform)
    CASE
      WHEN src_ts21 IS NULL THEN NULL
      WHEN TRIM(src_ts21) = '[]' THEN ''
      WHEN TRIM(src_ts21) LIKE '[%' AND TRIM(src_ts21) LIKE '%]'
        THEN REGEXP_REPLACE(
               REGEXP_REPLACE(
                 REGEXP_REPLACE(SUBSTR(TRIM(src_ts21), 2, LENGTH(TRIM(src_ts21)) - 2), '"', ''),
                 '\\s*,\\s*',
                 '|'
               ),
               '\\s+',
               ''
             )
      ELSE NULL
    END AS ts21
  FROM transform_demo.source_input
""")

case class TargetRecord(
  case_id: String,
  ts1: java.sql.Timestamp,
  ts2: java.lang.Short,
  ts3: BigDecimal,
  ts4: java.sql.Timestamp,
  ts5: String,
  ts6: String,
  ts7: BigDecimal,
  ts8: String,
  ts9: java.lang.Long,
  ts10: BigDecimal,
  ts11: java.lang.Double,
  ts12: BigDecimal,
  ts13: String,
  ts14: String,
  ts15: String,
  ts16: String,
  ts17: String,
  ts18: String,
  ts19: String,
  ts20: String,
  ts21: String
)

val typedDs = spark.sql("""
  SELECT case_id, ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8, ts9, ts10,
         ts11, ts12, ts13, ts14, ts15, ts16, ts17, ts18, ts19, ts20, ts21
  FROM transform_demo.transformed_view
  ORDER BY case_id
""").as[TargetRecord]

typedDs.show(false)

println(s"Pipeline finished. Parquet location: $parquetPath")
