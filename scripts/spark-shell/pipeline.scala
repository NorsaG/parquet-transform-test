import java.sql.Date
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField

spark.sparkContext.setLogLevel("WARN")
import spark.implicits._

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")

val parquetBasePath = sys.env.getOrElse("PARQUET_PATH", "/data/parquet/input")
val parquetPath = s"$parquetBasePath/run_${System.currentTimeMillis()}"

val schema = DataTypes.createStructType(Array[StructField](
  DataTypes.createStructField("src_ts1", DataTypes.DateType, true),
  DataTypes.createStructField("src_ts2", DataTypes.IntegerType, true),
  DataTypes.createStructField("src_ts3", DataTypes.StringType, true),
  DataTypes.createStructField("src_ts4", DataTypes.StringType, true),
  DataTypes.createStructField("src_ts5", DataTypes.StringType, true),
  DataTypes.createStructField("src_ts6", DataTypes.StringType, true),
  DataTypes.createStructField("src_ts7", DataTypes.StringType, true),
  DataTypes.createStructField("src_ts8", DataTypes.StringType, true),
  DataTypes.createStructField("src_ts9", DataTypes.StringType, true),
  DataTypes.createStructField("src_ts10", DataTypes.FloatType, true),
  DataTypes.createStructField("src_ts11", DataTypes.FloatType, true),
  DataTypes.createStructField("src_ts12", DataTypes.DoubleType, true),
  DataTypes.createStructField("src_ts13", DataTypes.StringType, true),
  DataTypes.createStructField("src_ts14", DataTypes.StringType, true),
  DataTypes.createStructField("src_ts15", DataTypes.StringType, true),
  DataTypes.createStructField("src_ts16", DataTypes.StringType, true),
  DataTypes.createStructField("src_ts17", DataTypes.DateType, true),
  DataTypes.createStructField("src_ts18", DataTypes.StringType, true),
  DataTypes.createStructField("src_ts19", DataTypes.StringType, true),
  DataTypes.createStructField("src_ts20", DataTypes.StringType, true),
  DataTypes.createStructField("src_ts21", DataTypes.StringType, true)
))

val rows = Seq(
  RowFactory.create(Date.valueOf("2026-01-15"), Int.box(120), "12345.678901234567", "2026-05-06 12:34:56.123456", "2026-05-06T12:34:56.123456789", "2026-05-06T12:34:56.123456+03:00", "999.99", "23:59:59.987654321", "9223372036854775806", Float.box(12.3456f), Float.box(7.5f), Double.box(456.789012d), "2026-05-06T12:34:56.123456+05:00", "2026-05-06T12:34:56+03:00", "2026-05-06T12:34:56+03:00[Europe/Moscow]", "2026-05-06T09:34:56Z[UTC]", Date.valueOf("2024-02-29"), "01:02:03.123456", "com.demo.Type:value42", "[{\"key\":\"k1\",\"type\":\"TypeA\"},{\"key\":\"k2\",\"type\":\"TypeB\"}]", "[[\"123\",\"12345\",null],[\"abc\",\"999\",\"x\"]]"),
  RowFactory.create(Date.valueOf("0001-01-01"), Int.box(1), "1.000000000001", "0001-01-01 00:00:00.000000", "0001-01-01T00:00:00.000000000", "0001-01-01T00:00:00.000000+00:00", "1.00", "00:00:00.000000000", "1", Float.box(1.0f), Float.box(1.0f), Double.box(1.0d), "0001-01-01T00:00:00+00:00", "0001-01-01T00:00:00+00:00", "0001-01-01T00:00:00+00:00[UTC]", "0001-01-01T00:00:00Z[UTC]", Date.valueOf("0001-01-01"), "00:00:00.000000000", "case.boundary:min", "[]", "[[\"min\",\"1\",null]]"),
  RowFactory.create(Date.valueOf("9999-12-31"), Int.box(2), "2.000000000002", "9999-12-31 23:59:59.999999", "9999-12-31T23:59:59.999999999", "9999-12-31T23:59:59.999999+00:00", "2.00", "23:59:59.999999999", "2", Float.box(2.0f), Float.box(2.0f), Double.box(2.0d), "9999-12-31T23:59:59.999999+00:00", "9999-12-31T23:59:59+00:00", "9999-12-31T23:59:59+00:00[UTC]", "9999-12-31T23:59:59Z[UTC]", Date.valueOf("9999-12-31"), "23:59:59.999999999", "case.boundary:max", "[]", "[[\"max\",\"2\",null]]"),
  RowFactory.create(Date.valueOf("2026-01-01"), Int.box(3), "3.000000000003", "2026-01-01 00:00:00.000000", "0000-01-01T00:00:00.000000000", "-0001-12-31T23:59:59.000000+00:00", "3.00", "25:61:61.000000000", "3", Float.box(3.0f), Float.box(3.0f), Double.box(3.0d), "10000-01-01T00:00:00+00:00", "0000-01-01T00:00:00+00:00", "-0001-12-31T23:59:59+00:00[UTC]", "10000-01-01T00:00:00Z[UTC]", Date.valueOf("2026-01-01"), "99:99:99.999999999", "case.outofrange:strings", "[]", "[[\"oor\",\"3\",null]]"),
  RowFactory.create(Date.valueOf("0001-01-01"), Int.box(-32768), "-0.000000000001", "1970-01-01 00:00:00.000001", "1999-12-31T23:59:59.999999999", "2026-05-06T12:34:56.999999+00:00", "-9999999999999.99", "00:00:00.000000001", "-9223372036854775808", Float.box(-1.25f), Float.box(0.0f), Double.box(-1.0d), "2026-05-06T12:34:56.000000Z", "2026-05-06T12:34:56Z", "2026-05-06T12:34:56Z[UTC]", "2026-05-06T12:34:56-07:00[America/Los_Angeles]", Date.valueOf("9999-12-31"), "23:59:59.999999999", "my.type.Name:value.with.dots", "[]", "[[\"123\",null,\"x\"]]"),
  RowFactory.create(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
)

val sourceDf = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
sourceDf.write.mode("errorifexists").parquet(parquetPath)

spark.sql("CREATE DATABASE IF NOT EXISTS transform_demo")
spark.sql("DROP TABLE IF EXISTS transform_demo.source_input")
spark.sql(s"""
  CREATE TABLE transform_demo.source_input (
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
  USING PARQUET
  LOCATION '$parquetPath'
""")

spark.sql("DROP VIEW IF EXISTS transform_demo.transformed_view")
spark.sql("""
  CREATE VIEW transform_demo.transformed_view AS
  SELECT
    CAST(src_ts1 AS TIMESTAMP) AS ts1,
    CAST(src_ts2 AS SMALLINT) AS ts2,
    CAST(src_ts3 AS DECIMAL(38,12)) AS ts3,
    TO_TIMESTAMP(src_ts4, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS ts4,
    SUBSTR(REGEXP_REPLACE(src_ts5, 'T', ' '), 1, 26) AS ts5,
    REGEXP_REPLACE(SUBSTR(REGEXP_REPLACE(src_ts6, 'T', ' '), 1, 29), '([+-][0-9]{2}):00$', '$1') AS ts6,
    CAST(src_ts7 AS DECIMAL(19,2)) AS ts7,
    SUBSTR(REGEXP_EXTRACT(src_ts8, '[0-9]{2}:[0-9]{2}:[0-9]{2}(\\\\.[0-9]+)?', 0), 1, 15) AS ts8,
    CAST(src_ts9 AS BIGINT) AS ts9,
    CAST(src_ts10 AS DECIMAL(38,12)) AS ts10,
    CAST(src_ts11 AS DOUBLE) AS ts11,
    CAST(src_ts12 AS DECIMAL(38,12)) AS ts12,
    REGEXP_REPLACE(REGEXP_REPLACE(src_ts13, 'T', ' '), '(Z|[+-][0-9]{2}:[0-9]{2})$', '') AS ts13,

    DATE_FORMAT(
      TO_UTC_TIMESTAMP(
        SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(src_ts14, 'Z$', '+00:00'), 'T', ' '), 1, 19),
        COALESCE(
          NULLIF(REGEXP_EXTRACT(REGEXP_REPLACE(src_ts14, 'Z$', '+00:00'), '([+-][0-9]{2}:[0-9]{2})$', 1), ''),
          'UTC'
        )
      ),
      'yyyy-MM-dd HH:mm:ss'
    ) AS ts14,

    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(src_ts15, '\\\\[.*\\\\]$', ''),
        'T',
        ' '
      ),
      '(Z|[+-][0-9]{2}:[0-9]{2})$',
      ''
    ) AS ts15,

    DATE_FORMAT(
      TO_UTC_TIMESTAMP(
        SUBSTR(REGEXP_REPLACE(src_ts16, 'T', ' '), 1, 19),
        COALESCE(
          NULLIF(REGEXP_EXTRACT(src_ts16, '\\[([^\\]]+)\\]$', 1), ''),
          NULLIF(REGEXP_EXTRACT(REGEXP_REPLACE(src_ts16, 'Z$', '+00:00'), '([+-][0-9]{2}:[0-9]{2})$', 1), ''),
          'UTC'
        )
      ),
      'yyyy-MM-dd HH:mm:ss'
    ) AS ts16,

    CONCAT(CAST(src_ts17 AS STRING), ' 00:00:00.000000') AS ts17,
    SUBSTR(REGEXP_EXTRACT(src_ts18, '[0-9]{2}:[0-9]{2}:[0-9]{2}(\\\\.[0-9]+)?', 0), 1, 15) AS ts18,
    CONCAT(REGEXP_REPLACE(SPLIT(src_ts19, ':')[0], '\\\\.', '_'), ':', SPLIT(src_ts19, ':')[1]) AS ts19,

    CONCAT_WS(
      ';',
      TRANSFORM(
        FROM_JSON(src_ts20, 'array<struct<key:string,type:string>>'),
        x -> CONCAT(
          COALESCE(x.key, ''),
          ':',
          COALESCE(x.type, ''),
          ':',
          CAST(CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) AS BIGINT) * 1000 AS STRING),
          ':I'
        )
      )
    ) AS ts20,

    CONCAT_WS(
      ';',
      TRANSFORM(
        FROM_JSON(src_ts21, 'array<array<string>>'),
        a -> CONCAT_WS('|', TRANSFORM(a, e -> COALESCE(e, 'null')))
      )
    ) AS ts21

  FROM transform_demo.source_input
""")

case class TargetRecord(
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
  SELECT ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8, ts9, ts10,
         ts11, ts12, ts13, ts14, ts15, ts16, ts17, ts18, ts19, ts20, ts21
  FROM transform_demo.transformed_view
""").as[TargetRecord]

typedDs.show(false)

println(s"Pipeline finished. Parquet location: $parquetPath")
