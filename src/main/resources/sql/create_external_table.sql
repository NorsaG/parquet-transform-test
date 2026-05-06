CREATE DATABASE IF NOT EXISTS transform_demo;
USE transform_demo;

DROP TABLE IF EXISTS source_input;
CREATE EXTERNAL TABLE source_input (
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
LOCATION '${PARQUET_PATH}';
