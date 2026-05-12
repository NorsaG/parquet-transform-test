import spark.implicits._

spark.sparkContext.setLogLevel("WARN")
spark.sql("USE transform_demo")

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

println("Reading transformed_view as typed dataset")
val targetDs = spark.sql("""
  SELECT case_id, ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8, ts9, ts10,
         ts11, ts12, ts13, ts14, ts15, ts16, ts17, ts18, ts19, ts20, ts21
  FROM transformed_view
  ORDER BY case_id
""").as[TargetRecord]

targetDs.show(false)

println("Readability checks for non-castable fields")

