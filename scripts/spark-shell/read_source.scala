spark.sparkContext.setLogLevel("WARN")

spark.sql("USE transform_demo")

println("Reading source_input from Spark/Hive catalog")
spark.sql("SELECT * FROM source_input").show(false)
println("source_input schema")
spark.table("source_input").printSchema()

