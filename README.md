# parquet-transform-test

Проект реализует pipeline из `src/main/resources/request.txt` с учетом обновленного критерия по колонкам.

## Что реализовано
- Генерация parquet с **21 входной колонкой** (`src_ts1` ... `src_ts21`) — по уникальным кодам трансформации TS1..TS21
- Hive SQL для создания external table и view с **21 выходной колонкой** (`ts1` ... `ts21`) и target-типами
- SQL-запросы для демонстрации типизированного чтения и невозможных cast
- Spark-shell сценарии для полного запуска и проверки source/target чтения
- Docker-окружение с объединенным сервисом Spark + Hive support и монтированием parquet

## Основные файлы
- Таблица правил: `src/main/resources/transformation-table.txt`
- Входной parquet generator (Java): `src/main/java/com/light/fdb/service/ParquetDataGenerator.java`
- Hive SQL:
  - `src/main/resources/sql/create_external_table.sql`
  - `src/main/resources/sql/create_transform_view.sql`
  - `src/main/resources/sql/demo_queries.sql`
  - `src/main/resources/sql/generate_parquet_data.sql`
  - `scripts/hive/select_from_hive.sql`
  - `scripts/hive/select_from_hive_beeline.sql`
  - `scripts/hive/select_datetime_edge_cases.sql`
  - `scripts/hive/select_datetime_edge_cases_beeline.sql`
  - `scripts/hive/run_select.sh`
  - `scripts/hive/run_select_datetime_edge_cases.sh`
- Spark-shell scripts:
  - `scripts/spark-shell/pipeline.scala`
  - `scripts/spark-shell/read_source.scala`
  - `scripts/spark-shell/read_target.scala`
  - `scripts/spark-shell/run_pipeline.sh`
  - `scripts/spark-shell/run_read_source.sh`
  - `scripts/spark-shell/run_read_target.sh`
- Docker: `docker-compose.yml`

## Docker запуск
```powershell
docker compose up -d
```

## Проверочные скрипты
1) Построение parquet + source table + target view:
```powershell
docker compose exec spark-hive bash /workspace/scripts/spark-shell/run_pipeline.sh
```

2) Выборка из Hive (через `spark-sql`):
```powershell
docker compose exec spark-hive bash /workspace/scripts/hive/run_select.sh
```

2.1) Выборка граничных кейсов дат/времени:
```powershell
docker compose exec spark-hive bash /workspace/scripts/hive/run_select_datetime_edge_cases.sh
```

3) Аналогичный запрос для beeline:
```powershell
docker compose exec spark-hive bash -lc "beeline -u jdbc:hive2://localhost:10000 -n hive -f /workspace/scripts/hive/select_from_hive_beeline.sql"
```

4) Чтение source-данных в spark-shell:
```powershell
docker compose exec spark-hive bash /workspace/scripts/spark-shell/run_read_source.sh
```

5) Чтение target-данных в spark-shell:
```powershell
docker compose exec spark-hive bash /workspace/scripts/spark-shell/run_read_target.sh
```

## Реализованные парсинги
В `src/main/resources/sql/create_transform_view.sql` реализованы функции парсинга для колонок, которые ранее были заглушками:
- `TS14`: OffsetDateTime -> UTC string
- `TS15`: ZonedDateTime -> string without zone/offset
- `TS16`: ZonedDateTime -> UTC string
- `TS20`: JSON array objects -> `key:type:timestamp:status` joined by `;`
- `TS21`: JSON array arrays -> values joined by `|`, rows joined by `;`

## Монтирование parquet
- Host: `./target/data/parquet/input`
- Container: `/data/parquet/input`

## Тесты
```powershell
mvn clean test
```
