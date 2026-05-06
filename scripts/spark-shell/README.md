# Spark-shell pipeline

Run environment:

```bash
docker compose up -d
```

Build source parquet + source table + target view:

```bash
docker compose exec spark-hive bash -lc "spark-shell -i /workspace/scripts/spark-shell/pipeline.scala"
```

Read source data in spark-shell:

```bash
docker compose exec spark-hive bash -lc "spark-shell -i /workspace/scripts/spark-shell/read_source.scala"
```

Read target data in spark-shell:

```bash
docker compose exec spark-hive bash -lc "spark-shell -i /workspace/scripts/spark-shell/read_target.scala"
```

Run Hive-style select script:

```bash
docker compose exec spark-hive bash -lc "spark-sql -f /workspace/scripts/hive/select_from_hive.sql"
```
