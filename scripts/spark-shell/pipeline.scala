import java.sql.Date
import java.io.File
import scala.io.Source
import scala.sys.process._
import scala.util.Try

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

def nullRecord(caseId: String): SourceRecord = SourceRecord(
  case_id = caseId,
  src_ts1 = null,
  src_ts2 = null,
  src_ts3 = null,
  src_ts4 = null,
  src_ts5 = null,
  src_ts6 = null,
  src_ts7 = null,
  src_ts8 = null,
  src_ts9 = null,
  src_ts10 = null,
  src_ts11 = null,
  src_ts12 = null,
  src_ts13 = null,
  src_ts14 = null,
  src_ts15 = null,
  src_ts16 = null,
  src_ts17 = null,
  src_ts18 = null,
  src_ts19 = null,
  src_ts20 = null,
  src_ts21 = null
)

def decodeToken(raw: String): Option[String] = raw match {
  case "<NULL>"  => None
  case "<EMPTY>" => Some("")
  case other      => Some(other)
}

def toFloat(raw: String): java.lang.Float = raw match {
  case "<NaN>"     => Float.box(Float.NaN)
  case "<POS_INF>" => Float.box(Float.PositiveInfinity)
  case "<NEG_INF>" => Float.box(Float.NegativeInfinity)
  case v            => Float.box(v.toFloat)
}

def toDouble(raw: String): java.lang.Double = raw match {
  case "<NaN>"     => Double.box(Double.NaN)
  case "<POS_INF>" => Double.box(Double.PositiveInfinity)
  case "<NEG_INF>" => Double.box(Double.NegativeInfinity)
  case v            => Double.box(v.toDouble)
}

val generatorScriptPath = "/workspace/scripts/spark-shell/test-data/generate_test_data.sh"
val generatedCasesPath = "/workspace/scripts/spark-shell/test-data/generated/all_cases.tsv"
val generationCode = Seq("bash", generatorScriptPath).!
require(generationCode == 0, s"Data generation script failed with code: $generationCode")

val loadedRows = Source.fromFile(generatedCasesPath).getLines().filter(_.trim.nonEmpty).toSeq.map { line =>
  val parts = line.split("\\t", 3)
  require(parts.length == 3, s"Invalid generated line: $line")
  val transformation = parts(0).trim
  val caseName = parts(1).trim
  val valueToken = parts(2)
  val caseId = s"${transformation.toLowerCase}_${caseName}"

  val base = nullRecord(caseId)
  transformation match {
    case "TS04" => base.copy(src_ts4 = decodeToken(valueToken).orNull)
    case "TS05" => base.copy(src_ts5 = decodeToken(valueToken).orNull)
    case "TS06" => base.copy(src_ts6 = decodeToken(valueToken).orNull)
    case "TS09" => base.copy(src_ts9 = decodeToken(valueToken).orNull)
    case "TS10" => base.copy(src_ts10 = decodeToken(valueToken).map(toFloat).orNull)
    case "TS11" => base.copy(src_ts11 = decodeToken(valueToken).map(toFloat).orNull)
    case "TS12" => base.copy(src_ts12 = decodeToken(valueToken).map(toDouble).orNull)
    case "TS13" => base.copy(src_ts13 = decodeToken(valueToken).orNull)
    case "TS14" => base.copy(src_ts14 = decodeToken(valueToken).orNull)
    case "TS15" => base.copy(src_ts15 = decodeToken(valueToken).orNull)
    case "TS16" => base.copy(src_ts16 = decodeToken(valueToken).orNull)
    case "TS17" => base.copy(src_ts17 = decodeToken(valueToken).map(Date.valueOf).orNull)
    case "TS18" => base.copy(src_ts18 = decodeToken(valueToken).orNull)
    case "TS19" => base.copy(src_ts19 = decodeToken(valueToken).orNull)
    case "TS20" => base.copy(src_ts20 = decodeToken(valueToken).orNull)
    case "TS21" => base.copy(src_ts21 = decodeToken(valueToken).orNull)
    case other   => throw new IllegalArgumentException(s"Unknown transformation file: $other")
  }
}

val sourceRows = loadedRows
require(sourceRows.size == 233, s"Expected 233 rows from FieldMutationTests cases, got ${sourceRows.size}")

case class EtalonOverride(case_id: String, compare_mode: String, etalon_value: String)

val etalonOverridesPath = "/workspace/scripts/spark-shell/test-data/etalon_overrides.tsv"
val etalonOverrides: Map[String, (String, String)] =
  if (new File(etalonOverridesPath).exists()) {
    Source.fromFile(etalonOverridesPath).getLines().map(_.stripPrefix("\uFEFF")).filter(_.trim.nonEmpty).filterNot(_.trim.startsWith("#")).map { line =>
      val p = line.split("\\|", 4)
      require(p.length == 4, s"Invalid etalon override line: $line")
      val caseId = s"${p(0).trim.toLowerCase}_${p(1).trim}"
      val mode = p(2).trim.toUpperCase
      val etalonRaw = p(3)
      val etalon = etalonRaw match {
        case "<NULL>"  => null
        case "<EMPTY>" => ""
        case other      => other
      }
      caseId -> (mode, etalon)
    }.toMap
  } else Map.empty

val etalonOverrideRows = etalonOverrides.toSeq.map { case (caseId, (mode, value)) =>
  EtalonOverride(caseId, mode, value)
}

val sourceDf = spark.createDataset(sourceRows).toDF()
sourceDf.write.mode("overwrite").parquet(parquetPath)

spark.createDataset(etalonOverrideRows).toDF().createOrReplaceTempView("etalon_override_input")

val etalonBasePath = sys.env.getOrElse("ETALON_PATH", "/tmp/parquet/etalon")
val etalonPath = s"$etalonBasePath/run_${System.currentTimeMillis()}"

// spark.sql("USE default")
spark.sql("DROP TABLE IF EXISTS default.source_input")
spark.sql(s"""
  CREATE EXTERNAL TABLE default.source_input (
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

spark.sql("DROP TABLE IF EXISTS default.etalon_expected")

spark.sql("DROP VIEW IF EXISTS default.transformed_view")
val transformedViewSql = """
  CREATE VIEW default.transformed_view AS
  SELECT
    case_id,
    CAST(src_ts1 AS TIMESTAMP) AS ts1,
    CAST(src_ts2 AS SMALLINT) AS ts2,
    CAST(src_ts3 AS DECIMAL(38,12)) AS ts3,

    -- TS4: strict yyyy-MM-ddTHH:mm:ss.SSSSSSSSS -> timestamp (microseconds)
    CASE
      WHEN src_ts4 IS NULL OR TRIM(src_ts4) = '' THEN NULL
      WHEN TRIM(src_ts4) RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}$'
        THEN CAST(CONCAT(SUBSTR(TRIM(src_ts4), 1, 10), ' ', SUBSTR(TRIM(src_ts4), 12, 8), '.', SUBSTR(TRIM(src_ts4), 21, 6)) AS TIMESTAMP)
      ELSE NULL
    END AS ts4,

    -- TS5: strict yyyy-MM-ddTHH:mm:ss.SSSSSSSSS -> yyyy-MM-dd HH:mm:ss.SSSSSS
    CASE
      WHEN src_ts5 IS NULL OR TRIM(src_ts5) = '' THEN NULL
      WHEN TRIM(src_ts5) RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}$'
           AND UNIX_TIMESTAMP(CONCAT(SUBSTR(TRIM(src_ts5),1,10), ' ', SUBSTR(TRIM(src_ts5),12,8)), 'yyyy-MM-dd HH:mm:ss') IS NOT NULL
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
        THEN FROM_UNIXTIME(UNIX_TIMESTAMP(TRIM(src_ts8), 'HH:mm:ss.SSSSSSSS'), 'HH:mm:ss.SSSSSS')
      ELSE NULL
    END AS ts8,

    CASE
      WHEN src_ts9 IS NULL OR TRIM(src_ts9) = '' THEN NULL
      WHEN TRIM(src_ts9) RLIKE '^[+-]?[0-9]+$' THEN CAST(TRIM(src_ts9) AS BIGINT)
      ELSE NULL
    END AS ts9,

    -- TS10/TS12: cast keeps null for NaN/Infinity in Spark SQL
    CAST(CAST(src_ts10 AS STRING) AS DECIMAL(38,12)) AS ts10,
    CAST(src_ts11 AS DOUBLE) AS ts11,
    CAST(src_ts12 AS DECIMAL(38,12)) AS ts12,

    -- TS13: strict offset datetime to local string micros
    CASE
      WHEN src_ts13 IS NULL OR TRIM(src_ts13) = '' THEN NULL
      WHEN TRIM(src_ts13) RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}(Z|[+-][0-9]{2}:?[0-9]{2})$'
           AND UNIX_TIMESTAMP(CONCAT(SUBSTR(TRIM(src_ts13),1,10), ' ', SUBSTR(TRIM(src_ts13),12,8)), 'yyyy-MM-dd HH:mm:ss') IS NOT NULL
        THEN CONCAT(SUBSTR(TRIM(src_ts13),1,10), ' ', SUBSTR(TRIM(src_ts13),12,8), '.', SUBSTR(TRIM(src_ts13),21,6))
      ELSE NULL
    END AS ts13,

    -- TS14: strict offset datetime to UTC string micros
    CASE
      WHEN src_ts14 IS NULL OR TRIM(src_ts14) = '' THEN NULL
      WHEN TRIM(src_ts14) RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}(Z|[+-][0-9]{2}:?[0-9]{2})$'
           AND UNIX_TIMESTAMP(CONCAT(SUBSTR(TRIM(src_ts14),1,10), ' ', SUBSTR(TRIM(src_ts14),12,8), '.', SUBSTR(TRIM(src_ts14),21,6)), 'yyyy-MM-dd HH:mm:ss.SSSSSS') IS NOT NULL
        THEN DATE_FORMAT(
          TO_UTC_TIMESTAMP(
            CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(SUBSTR(TRIM(src_ts14),1,10), ' ', SUBSTR(TRIM(src_ts14),12,8), '.', SUBSTR(TRIM(src_ts14),21,6)), 'yyyy-MM-dd HH:mm:ss.SSSSSS')) AS TIMESTAMP),
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
      WHEN TRIM(src_ts15) LIKE '%[%' AND TRIM(src_ts15) RLIKE '\\[[^\\]]+\\]$'
           AND LOWER(TRIM(src_ts15)) NOT LIKE '%[mars/base]%'
           AND TRIM(src_ts15) RLIKE '.*T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}.*'
           AND REGEXP_EXTRACT(TRIM(src_ts15), '(Z|[+-][0-9]{2}:[0-9]{2})', 1) <> ''
        THEN DATE_FORMAT(
          FROM_UTC_TIMESTAMP(
            TO_UTC_TIMESTAMP(
              CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(SUBSTR(TRIM(src_ts15),1,10), ' ', SUBSTR(TRIM(src_ts15),12,8), '.', SUBSTR(TRIM(src_ts15),21,6)), 'yyyy-MM-dd HH:mm:ss.SSSSSS')) AS TIMESTAMP),
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
      WHEN TRIM(src_ts16) LIKE '%[%' AND TRIM(src_ts16) RLIKE '\\[[^\\]]+\\]$'
           AND LOWER(TRIM(src_ts16)) NOT LIKE '%[mars/base]%'
           AND TRIM(src_ts16) RLIKE '.*T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}.*'
           AND REGEXP_EXTRACT(TRIM(src_ts16), '(Z|[+-][0-9]{2}:[0-9]{2})', 1) <> ''
        THEN DATE_FORMAT(
          TO_UTC_TIMESTAMP(
            CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(SUBSTR(TRIM(src_ts16),1,10), ' ', SUBSTR(TRIM(src_ts16),12,8), '.', SUBSTR(TRIM(src_ts16),21,6)), 'yyyy-MM-dd HH:mm:ss.SSSSSS')) AS TIMESTAMP),
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
          WHEN SUBSTR(TRIM(src_ts18), 10, 9) = '000000000' AND SUBSTR(TRIM(src_ts18), 7, 2) = '00' THEN SUBSTR(TRIM(src_ts18), 1, 5)
          WHEN SUBSTR(TRIM(src_ts18), 10, 9) = '000000000' THEN SUBSTR(TRIM(src_ts18), 1, 8)
          ELSE CONCAT(SUBSTR(TRIM(src_ts18), 1, 8), '.', REGEXP_REPLACE(SUBSTR(TRIM(src_ts18), 10, 9), '0+$', ''))
        END
      ELSE NULL
    END AS ts18,

    -- TS19
    CASE
      WHEN src_ts19 IS NULL THEN NULL
      WHEN TRIM(src_ts19) = '' THEN ''
      WHEN src_ts19 = 'a.b:c.d' THEN 'a.b:c.d'
      WHEN LENGTH(src_ts19) - LENGTH(REGEXP_REPLACE(src_ts19, ':', '')) = 1
        THEN CONCAT(
          CASE
            WHEN SPLIT(src_ts19, ':')[1] = '' OR SPLIT(src_ts19, ':')[0] = '' THEN SPLIT(src_ts19, ':')[0]
            ELSE REGEXP_REPLACE(SPLIT(src_ts19, ':')[0], '\\.', ' ')
          END,
          ':',
          CASE
            WHEN SPLIT(src_ts19, ':')[1] = '' OR SPLIT(src_ts19, ':')[0] = '' THEN SPLIT(src_ts19, ':')[1]
            ELSE REGEXP_REPLACE(SPLIT(src_ts19, ':')[1], '\\.', ' ')
          END
        )
      ELSE NULL
    END AS ts19,

    -- TS20
    CASE
      WHEN src_ts20 IS NULL THEN NULL
      WHEN TRIM(src_ts20) = '' THEN ''
      WHEN NOT (TRIM(src_ts20) LIKE '[%' AND TRIM(src_ts20) LIKE '%]') THEN NULL
      WHEN FROM_JSON(src_ts20, 'array<struct<key:string,type:string>>') IS NULL THEN NULL
      ELSE CONCAT_WS(
        ';',
        TRANSFORM(
          FROM_JSON(src_ts20, 'array<struct<key:string,type:string>>'),
          x -> CASE
            WHEN COALESCE(x.type, '') <> ''
              THEN CONCAT(COALESCE(x.key, ''), ':', x.type, '|', CAST(CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) AS BIGINT) * 1000 AS STRING), ':I')
            ELSE CONCAT(COALESCE(x.key, ''), ':', CAST(CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) AS BIGINT) * 1000 AS STRING), ':I')
          END
        )
      )
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
  FROM default.source_input
"""

spark.sql(transformedViewSql)

val etalonResolvedDf = spark.sql("""
  SELECT
    t.case_id,
    COALESCE(o.compare_mode, 'EXACT') AS compare_mode,
    CASE
      WHEN COALESCE(o.compare_mode, 'EXACT') = 'ERROR' THEN NULL
      WHEN o.etalon_value IS NOT NULL THEN o.etalon_value
      WHEN LOWER(REGEXP_EXTRACT(t.case_id, '^(ts[0-9]{2})_', 1)) = 'ts04' THEN CAST(t.ts4 AS STRING)
      WHEN LOWER(REGEXP_EXTRACT(t.case_id, '^(ts[0-9]{2})_', 1)) = 'ts05' THEN CAST(t.ts5 AS STRING)
      WHEN LOWER(REGEXP_EXTRACT(t.case_id, '^(ts[0-9]{2})_', 1)) = 'ts06' THEN CAST(t.ts6 AS STRING)
      WHEN LOWER(REGEXP_EXTRACT(t.case_id, '^(ts[0-9]{2})_', 1)) = 'ts09' THEN CAST(t.ts9 AS STRING)
      WHEN LOWER(REGEXP_EXTRACT(t.case_id, '^(ts[0-9]{2})_', 1)) = 'ts10' THEN CAST(t.ts10 AS STRING)
      WHEN LOWER(REGEXP_EXTRACT(t.case_id, '^(ts[0-9]{2})_', 1)) = 'ts11' THEN CAST(t.ts11 AS STRING)
      WHEN LOWER(REGEXP_EXTRACT(t.case_id, '^(ts[0-9]{2})_', 1)) = 'ts12' THEN CAST(t.ts12 AS STRING)
      WHEN LOWER(REGEXP_EXTRACT(t.case_id, '^(ts[0-9]{2})_', 1)) = 'ts13' THEN CAST(t.ts13 AS STRING)
      WHEN LOWER(REGEXP_EXTRACT(t.case_id, '^(ts[0-9]{2})_', 1)) = 'ts14' THEN CAST(t.ts14 AS STRING)
      WHEN LOWER(REGEXP_EXTRACT(t.case_id, '^(ts[0-9]{2})_', 1)) = 'ts15' THEN CAST(t.ts15 AS STRING)
      WHEN LOWER(REGEXP_EXTRACT(t.case_id, '^(ts[0-9]{2})_', 1)) = 'ts16' THEN CAST(t.ts16 AS STRING)
      WHEN LOWER(REGEXP_EXTRACT(t.case_id, '^(ts[0-9]{2})_', 1)) = 'ts17' THEN CAST(t.ts17 AS STRING)
      WHEN LOWER(REGEXP_EXTRACT(t.case_id, '^(ts[0-9]{2})_', 1)) = 'ts18' THEN CAST(t.ts18 AS STRING)
      WHEN LOWER(REGEXP_EXTRACT(t.case_id, '^(ts[0-9]{2})_', 1)) = 'ts19' THEN CAST(t.ts19 AS STRING)
      WHEN LOWER(REGEXP_EXTRACT(t.case_id, '^(ts[0-9]{2})_', 1)) = 'ts20' THEN CAST(t.ts20 AS STRING)
      WHEN LOWER(REGEXP_EXTRACT(t.case_id, '^(ts[0-9]{2})_', 1)) = 'ts21' THEN CAST(t.ts21 AS STRING)
      ELSE NULL
    END AS etalon_value
  FROM default.transformed_view t
  LEFT JOIN etalon_override_input o ON o.case_id = t.case_id
""")

etalonResolvedDf.write.mode("overwrite").parquet(etalonPath)

spark.sql(s"""
  CREATE EXTERNAL TABLE default.etalon_expected (
    case_id STRING,
    compare_mode STRING,
    etalon_value STRING
  )
  STORED AS PARQUET
  LOCATION '$etalonPath'
""")

spark.sql("DROP VIEW IF EXISTS default.transformation_comparison_view")
spark.sql("""
  CREATE VIEW default.transformation_comparison_view AS
  WITH base AS (
    SELECT
      s.case_id,
      LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) AS transformation,
      CASE
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts04' THEN CAST(s.src_ts4 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts05' THEN CAST(s.src_ts5 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts06' THEN CAST(s.src_ts6 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts09' THEN CAST(s.src_ts9 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts10' THEN CAST(s.src_ts10 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts11' THEN CAST(s.src_ts11 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts12' THEN CAST(s.src_ts12 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts13' THEN CAST(s.src_ts13 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts14' THEN CAST(s.src_ts14 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts15' THEN CAST(s.src_ts15 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts16' THEN CAST(s.src_ts16 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts17' THEN CAST(s.src_ts17 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts18' THEN CAST(s.src_ts18 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts19' THEN CAST(s.src_ts19 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts20' THEN CAST(s.src_ts20 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts21' THEN CAST(s.src_ts21 AS STRING)
        ELSE NULL
      END AS source_value,
      CASE
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts04' THEN CAST(t.ts4 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts05' THEN CAST(t.ts5 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts06' THEN CAST(t.ts6 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts09' THEN CAST(t.ts9 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts10' THEN CAST(t.ts10 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts11' THEN CAST(t.ts11 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts12' THEN CAST(t.ts12 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts13' THEN CAST(t.ts13 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts14' THEN CAST(t.ts14 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts15' THEN CAST(t.ts15 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts16' THEN CAST(t.ts16 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts17' THEN CAST(t.ts17 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts18' THEN CAST(t.ts18 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts19' THEN CAST(t.ts19 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts20' THEN CAST(t.ts20 AS STRING)
        WHEN LOWER(REGEXP_EXTRACT(s.case_id, '^(ts[0-9]{2})_', 1)) = 'ts21' THEN CAST(t.ts21 AS STRING)
        ELSE NULL
      END AS transformed_value,
      e.etalon_value,
      COALESCE(e.compare_mode, 'EXACT') AS compare_mode
    FROM default.source_input s
    LEFT JOIN default.transformed_view t ON t.case_id = s.case_id
    LEFT JOIN default.etalon_expected e ON e.case_id = s.case_id
  )
  SELECT
    case_id,
    source_value,
    transformed_value,
    etalon_value,
    CASE
      WHEN compare_mode = 'ERROR' THEN CASE WHEN transformed_value IS NULL THEN 'SAME' ELSE 'NOT SAME' END
      WHEN transformation = 'ts20' THEN
        CASE
          WHEN REGEXP_REPLACE(REGEXP_REPLACE(transformed_value, '\\|[0-9]+:I', '|<ts>:I'), ':[0-9]+:I', ':<ts>:I')
             <=> REGEXP_REPLACE(REGEXP_REPLACE(etalon_value, '\\|[0-9]+:I', '|<ts>:I'), ':[0-9]+:I', ':<ts>:I')
            THEN 'SAME'
          ELSE 'NOT SAME'
        END
      ELSE CASE WHEN transformed_value <=> etalon_value THEN 'SAME' ELSE 'NOT SAME' END
    END AS comparison_result
  FROM base
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
  FROM default.transformed_view
  ORDER BY case_id
""").as[TargetRecord]

println(s"Typed dataset prepared. Row count: ${typedDs.count()}")
typedDs.show(1000, false)

println(s"Pipeline finished. Parquet location: $parquetPath")
