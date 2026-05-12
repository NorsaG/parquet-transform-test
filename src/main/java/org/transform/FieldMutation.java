package org.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.ArrayType;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;

import static java.math.RoundingMode.HALF_UP;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.apache.spark.sql.types.DataTypes.StringType;

public enum FieldMutation {

    TS1 {
        /**
         * Конвертация Date (INT96 в Parquet) в Timestamp
         */
        @Override
        public Column mutate(String fieldName) {
            return col(fieldName).cast(TimestampType).as(fieldName);
        }

        @Override
        public String columnType(String columnType) {
            return TIMESTAMP_TYPE;
        }
    },

    TS2 {
        /**
         * Конвертация Integer в SmallInt (ShortType)
         */
        @Override
        public Column mutate(String fieldName) {
            return col(fieldName).cast(ShortType).as(fieldName);
        }

        @Override
        public String columnType(String columnType) {
            return SMALLINT_TYPE;
        }
    },

    TS3 {
        /**
         * Конвертация String в Decimal(38,12)
         */
        @Override
        public Column mutate(String fieldName) {
            return col(fieldName).cast("decimal(38,12)").as(fieldName);
        }

        @Override
        public String columnType(String columnType) {
            return DECIMAL_TYPE;
        }
    },

    TS4 {
        /**
         * Приведение String к Timestamp с точностью до микросекунд
         */
        @Override
        public Column mutate(String fieldName) {
            UserDefinedFunction udf = udf((String value) -> {
                if (value == null || value.isBlank()) {
                    return null;
                }
                var ldt = LocalDateTime.parse(value.trim(), INPUT_LOCAL_DATE_TIME);
                long micros = (ldt.getNano() / 1000) * 1000L;
                var ldtMicros = ldt.withNano((int) micros);
                return Timestamp.from(ldtMicros.toInstant(UTC));
            }, TimestampType);
            return udf.apply(col(fieldName)).as(fieldName);
        }

        @Override
        public String columnType(String columnType) {
            return TIMESTAMP_TYPE;
        }
    },

    TS5 {
        /**
         * T9: Конвертация String ISO-8601 в String "yyyy-MM-dd HH:mm:ss.SSSSSS"
         */
        @Override
        public Column mutate(String fieldName) {
            UserDefinedFunction udf = udf((String value) -> {
                if (value == null || value.isBlank()) {
                    return null;
                }
                return LocalDateTime.parse(value.trim(), INPUT_LOCAL_DATE_TIME)
                        .format(OUTPUT_YYYY_MM_DD_HH_MM_SS_SSSSSS);
            }, StringType);
            return udf.apply(col(fieldName)).as(fieldName);
        }

        @Override
        public String columnType(String columnType) {
            return STRING_TYPE;
        }
    },

    TS6 {
        @Override
        public Column mutate(String fieldName) {
            Column c = col(fieldName);

            Column dateTime = regexp_replace(
                    regexp_extract(c, "^(\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}\\.\\d{6,})", 1),
                    "T", " "
            );

            Column rawOffset = regexp_extract(c, "(Z|[+-]\\d{2}:?\\d{2})", 1);

            Column offset = when(rawOffset.equalTo(""),
                    when(c.contains("Europe/Moscow"), lit("+03")).otherwise(lit("+00"))
            ).otherwise(rawOffset);

            Column finalOffset = regexp_replace(
                    regexp_replace(offset, "Z", "+00"),
                    ":00$", ""
            );

            return concat(dateTime, finalOffset).as(fieldName);
        }

        @Override
        public String columnType(String columnType) {
            return STRING_TYPE;
        }
    },

    TS7 {
        /**
         * Конвертация String в Decimal(19,2)
         */
        @Override
        public Column mutate(String fieldName) {
            return col(fieldName).cast("decimal(19,2)").as(fieldName);
        }

        @Override
        public String columnType(String columnType) {
            return DECIMAL_TYPE;
        }
    },

    TS8 {
        /**
         * Конвертация строки времени HH:mm:ss.SSSSSSSS в формат с микросекундами: HH:mm:ss.SSSSSS
         */
        @Override
        public Column mutate(String fieldName) {
            return date_format(
                    to_timestamp(col(fieldName), "HH:mm:ss.SSSSSSSS"),
                    "HH:mm:ss.SSSSSS"
            ).as(fieldName);
        }

        @Override
        public String columnType(String columnType) {
            return STRING_TYPE;
        }
    },

    TS9 {
        /**
         * T1: Конвертация String -> BigInteger -> Long
         */
        @Override
        public Column mutate(String fieldName) {
            UserDefinedFunction udf = udf((String value) -> {
                if (value == null || value.isBlank()) {
                    return null;
                }
                return new java.math.BigInteger(value.trim()).longValueExact();
            }, LongType);

            return udf.apply(col(fieldName)).as(fieldName);
        }

        @Override
        public String columnType(String columnType) {
            return LONG_TYPE;
        }
    },

    TS10 {
        /**
         * T2: Конвертация Float (в т.ч. scientific notation) в Decimal(38,12)
         * Выбрасывает исключение на NaN и Infinity
         */
        @Override
        public Column mutate(String fieldName) {
            UserDefinedFunction toDecimal = udf((Float value) -> {
                if (value == null) {
                    return null;
                }
                if (Float.isNaN(value) || Float.isInfinite(value)) {
                    throw new IllegalArgumentException("Value " + value + " is not supported for Decimal conversion");
                }
                return new BigDecimal(value.toString()).setScale(12, HALF_UP);
            }, createDecimalType(38, 12));

            return toDecimal.apply(col(fieldName)).as(fieldName);
        }

        @Override
        public String columnType(String columnType) {
            return DECIMAL_TYPE;
        }
    },

    TS11 {
        /**
         * T3: Float -> Double
         */
        @Override
        public Column mutate(String fieldName) {
            return col(fieldName).cast(DoubleType).as(fieldName);
        }

        @Override
        public String columnType(String columnType) {
            return DOUBLE_TYPE;
        }
    },

    TS12 {
        /**
         * T4: Double -> Decimal(38,12). NaN и Infinity не поддерживаются.
         */
        @Override
        public Column mutate(String fieldName) {
            UserDefinedFunction toDecimal = udf((Double value) -> {
                if (value == null) return null;
                if (Double.isNaN(value) || Double.isInfinite(value)) {
                    throw new IllegalArgumentException("Value " + value + " is not supported for Decimal conversion");
                }
                return new BigDecimal(value.toString()).setScale(12, HALF_UP);
            }, createDecimalType(38, 12));

            return toDecimal.apply(col(fieldName)).as(fieldName);
        }

        @Override
        public String columnType(String columnType) {
            return DECIMAL_TYPE;
        }
    },

    TS13 {
        /**
         * T5: String (ISO Offset) -> OffsetDateTime -> String "yyyy-MM-dd HH:mm:ss.SSSSSS"
         */
        @Override
        public Column mutate(String fieldName) {
            UserDefinedFunction formatWithMicros = udf((String value) -> {
                if (value == null || value.isBlank()) {
                    return null;
                }
                return OffsetDateTime.parse(value.trim(), INPUT_OFFSET_DATE_TIME)
                        .format(OUTPUT_YYYY_MM_DD_HH_MM_SS_SSSSSS);
            }, StringType);

            return formatWithMicros.apply(col(fieldName)).as(fieldName);
        }

        @Override
        public String columnType(String columnType) {
            return STRING_TYPE;
        }
    },

    TS14 {
        /**
         * T6: String -> OffsetDateTime -> Convert to UTC -> String "yyyy-MM-dd HH:mm:ss.SSSSSS"
         */
        @Override
        public Column mutate(String fieldName) {
            UserDefinedFunction toUtcMicros = udf((String value) -> {
                if (value == null || value.isBlank()) {
                    return null;
                }
                return OffsetDateTime.parse(value.trim(), INPUT_OFFSET_DATE_TIME)
                        .withOffsetSameInstant(UTC)
                        .format(OUTPUT_YYYY_MM_DD_HH_MM_SS_SSSSSS);
            }, StringType);

            return toUtcMicros.apply(col(fieldName)).as(fieldName);
        }

        @Override
        public String columnType(String columnType) {
            return STRING_TYPE;
        }
    },

    TS15 {
        /**
         * T7: String (ISO + [Zone]) -> ZonedDateTime -> String "yyyy-MM-dd HH:mm:ss.SSSSSS"
         */
        @Override
        public Column mutate(String fieldName) {
            UserDefinedFunction formatAsIs = udf((String value) -> {
                if (value == null || value.isBlank()) {
                    return null;
                }
                return ZonedDateTime.parse(value.trim(), INPUT_ZONED_DATE_TIME)
                        .format(OUTPUT_YYYY_MM_DD_HH_MM_SS_SSSSSS);
            }, StringType);

            return formatAsIs.apply(col(fieldName)).as(fieldName);
        }

        @Override
        public String columnType(String columnType) {
            return STRING_TYPE;
        }
    },

    TS16 {
        /**
         * T8: String (ZonedDateTime) -> Convert to UTC -> String "yyyy-MM-dd HH:mm:ss.SSSSSS"
         */
        @Override
        public Column mutate(String fieldName) {
            UserDefinedFunction toUtc = udf((String value) -> {
                if (value == null || value.isBlank()) {
                    return null;
                }
                return ZonedDateTime.parse(value.trim(), INPUT_ZONED_DATE_TIME)
                        .withZoneSameInstant(UTC)
                        .format(OUTPUT_YYYY_MM_DD_HH_MM_SS_SSSSSS);
            }, StringType);

            return toUtc.apply(col(fieldName)).as(fieldName);
        }

        @Override
        public String columnType(String columnType) {
            return STRING_TYPE;
        }
    },

    TS17 {
        /**
         * T10: DateType -> String "yyyy-MM-dd HH:mm:ss.SSSSSS"
         */
        @Override
        public Column mutate(String fieldName) {
            return date_format(col(fieldName), OUTPUT_YYYY_MM_DD_HH_MM_SS_SSSSSS).as(fieldName);
        }

        @Override
        public String columnType(String columnType) {
            return STRING_TYPE;
        }
    },

    TS18 {
        /**
         * T13: String -> LocalTime (STRICT) -> localTime.toString()
         */
        @Override
        public Column mutate(String fieldName) {
            UserDefinedFunction localTimeStr = udf((String value) -> {
                if (value == null || value.isBlank()) {
                    return null;
                }
                return LocalTime.parse(value.trim(), INPUT_TIME).toString();
            }, StringType);

            return localTimeStr.apply(col(fieldName)).as(fieldName);
        }

        @Override
        public String columnType(String columnType) {
            return STRING_TYPE;
        }
    },

    TS19 {
        /**
         * T14: REFERENCE
         */
        @Override
        public Column mutate(String fieldName) {
            UserDefinedFunction udf = udf((String reference) -> {
                if (reference == null) {
                    return null;
                }

                if (reference.isBlank()) {
                    return "";
                }

                long colonCount = reference.chars().filter(ch -> ch == ':').count();
                if (colonCount != 1) {
                    throw new IllegalArgumentException("Строка должна содержать ровно одно двоеточие: " + reference);
                }

                String[] parts = reference.split(":", -1);
                String type = parts[0].replace('.', ' ');
                return type + ":" + parts[1];
            }, StringType);

            return udf.apply(col(fieldName)).as(fieldName);
        }

        @Override
        public String columnType(String columnType) {
            return STRING_TYPE;
        }
    },

    TS20 {
        /**
         * T15: REFERENCE_COLLECTION
         */
        @Override
        public Column mutate(String fieldName) {
            UserDefinedFunction jsonParser = udf((String json) -> {
                if (json == null) {
                    return null;
                }
                if (json.isBlank()) {
                    return "";
                }

                try {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode nodes = mapper.readTree(json);
                    if (!nodes.isArray()) {
                        throw new IllegalArgumentException("JSON должен быть массивом: " + json);
                    }

                    long timestamp = System.currentTimeMillis();
                    ArrayList<String> results = new ArrayList<>();

                    for (JsonNode node : nodes) {
                        String key = node.path("key").asText("");
                        String type = node.path("type").asText("");
                        results.add(String.format("%s:%s:%d:I", key, type, timestamp));
                    }

                    return String.join(";", results);
                } catch (Exception e) {
                    throw new IllegalStateException("Ошибка парсинга JSON: " + json, e);
                }
            }, StringType);

            return jsonParser.apply(col(fieldName)).as(fieldName);
        }

        @Override
        public String columnType(String columnType) {
            return STRING_TYPE;
        }
    },

    TS21 {
        /**
         * T16: PRIMITIVE_COLLECTION
         * ["123", "456", null] -> "123|456|null"
         */
        @Override
        public Column mutate(String fieldName) {
            ArrayType arraySchema = createArrayType(StringType);
            return when(col(fieldName).isNull(), lit(null))
                    .otherwise(
                            array_join(
                                    transform(
                                            from_json(col(fieldName), arraySchema),
                                            x -> when(x.isNull(), lit("null")).otherwise(x)
                                    ),
                                    "|"
                            )
                    ).as(fieldName);
        }

        @Override
        public String columnType(String columnType) {
            return STRING_TYPE;
        }
    };

    public abstract Column mutate(String fieldName);
    public abstract String columnType(String columnType);

    // ---- helpers/consts expected elsewhere in the original file ----
    private static final String TIMESTAMP_TYPE = "timestamp";
    private static final String SMALLINT_TYPE = "smallint";
    private static final String DECIMAL_TYPE = "decimal";
    private static final String STRING_TYPE = "string";
    private static final String LONG_TYPE = "bigint";
    private static final String DOUBLE_TYPE = "double";

    private static final java.time.ZoneOffset UTC = java.time.ZoneOffset.UTC;
    private static final java.time.format.DateTimeFormatter INPUT_LOCAL_DATE_TIME =
            java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    private static final java.time.format.DateTimeFormatter INPUT_OFFSET_DATE_TIME =
            java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
    private static final java.time.format.DateTimeFormatter INPUT_ZONED_DATE_TIME =
            java.time.format.DateTimeFormatter.ISO_ZONED_DATE_TIME;
    private static final java.time.format.DateTimeFormatter INPUT_TIME =
            java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSS");
    private static final java.time.format.DateTimeFormatter OUTPUT_YYYY_MM_DD_HH_MM_SS_SSSSSS =
            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private static final ObjectMapper MAPPER = new ObjectMapper();
}