package org.transform;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.sql.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static javolution.testing.TestContext.assertEquals;
import static javolution.testing.TestContext.assertTrue;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.junit.jupiter.api.Assertions.*;
import static org.transform.FieldMutation.*;

class FieldMutationTests extends SparkTestBase {

    // ---------------- TS04 ----------------

    @ParameterizedTest(name = "TS4 (Strict UUID Timestamp) [{index}]: {0}")
    @MethodSource("provideTS4TestData")
    void testTS4_strictTimestamp(String desc, String input, String expectedTimestamp) {
        StructType schema = createStructType(List.of(createStructField("raw", DataTypes.StringType, true)));
        Dataset<Row> df = spark.createDataFrame(List.of(RowFactory.create(input)), schema);
        Dataset<Row> result = df.withColumn("mutated", TS4.mutate("raw"));

        if ("error".equals(expectedTimestamp)) {
            assertThrows(SparkException.class, result::collectAsList);
        } else {
            Row row = result.collectAsList().get(0);
            if (expectedTimestamp == null) {
                assertTrue(row.isNullAt(1));
            } else {
                assertEquals(expectedTimestamp, row.getTimestamp(1).toString(), desc);
            }
        }
    }

    static Stream<Arguments> provideTS4TestData() {
        return Stream.of(
            Arguments.of("Обычный", "2024-05-20T10:15:30.123456789", "2024-05-20 10:15:30.123456"),
            Arguments.of("Полночь", "2024-01-01T00:00:00.000000000", "2024-01-01 00:00:00.0"),
            Arguments.of("нулевые микро", "2024-05-20T10:15:30.000000999", "2024-05-20 10:15:30.0"),
            Arguments.of("Null", null, null),
            Arguments.of("Ошибка: формат", "2024-05-20 10:15:30.123456789", "error")
        );
    }

    // ---------------- TS05 ----------------

    @ParameterizedTest(name = "TS5 (Strict LocalDateTime String) [{index}]: {0}")
    @MethodSource("provideTS5TestData")
    void testTS5_strictLocalDateTimeString(String desc, String input, String expected) {
        StructType schema = createStructType(List.of(createStructField("raw", DataTypes.StringType, true)));
        Dataset<Row> df = spark.createDataFrame(List.of(RowFactory.create(input)), schema);
        Dataset<Row> result = df.withColumn("mutated", TS5.mutate("raw"));

        if ("error".equals(expected)) {
            assertThrows(SparkException.class, result::collectAsList,
                "Ожидалась ошибка для некорректного LocalDateTime: " + input);
        } else {
            Row row = result.collectAsList().get(0);
            if (expected == null) {
                assertTrue(row.isNullAt(1));
            } else {
                assertEquals(expected, row.getString(1), "Ошибка форматирования для: " + input);
            }
        }
    }

    static Stream<Arguments> provideTS5TestData() {
        return Stream.of(
            Arguments.of("Стандарт", "2024-05-20T10:15:30.123456789", "2024-05-20 10:15:30.123456"),
            Arguments.of("Минимум нано", "2024-05-20T10:15:30.000000001", "2024-05-20 10:15:30.000000"),
            Arguments.of("Максимум нано", "2024-12-31T23:59:59.999999999", "2024-12-31 23:59:59.999999"),
            Arguments.of("Полночь", "2024-01-01T00:00:00.000000000", "2024-01-01 00:00:00.000000"),
            Arguments.of("С пробелами", " 2024-05-20T10:15:30.123456789 ", "2024-05-20 10:15:30.123456"),
            Arguments.of("Null", null, null),
            Arguments.of("Пустая строка", "", null),
            Arguments.of("Пробелы вместо даты", "   ", null),
            Arguments.of("Мало знаков нано (3)", "2024-05-20T10:15:30.123", "error"),
            Arguments.of("Много знаков нано (12)", "2024-05-20T10:15:30.123456789012", "error"),
            Arguments.of("Присутствует зона", "2024-05-20T10:15:30.123456789Z", "error"),
            Arguments.of("Присутствует оффсет", "2024-05-20T10:15:30.123456789+03:00", "error"),
            Arguments.of("Разделитель пробел", "2024-05-20 10:15:30.123456789", "error"),
            Arguments.of("Невалидный час", "2024-05-20T25:15:30.123456789", "error"),
            Arguments.of("Дата без времени", "2024-05-20", "error"),
            Arguments.of("Время без даты", "10:15:30.123456789", "error"),
            Arguments.of("Случайный текст", "not-a-date-at-all", "error"),
            Arguments.of("Ф��рмат с косой чертой", "2024/05/20T10:15:30.123456789", "error"),
            Arguments.of("Двойная точка в нано", "2024-05-20T10:15:30..123456789", "error")
        );
    }

    // ---------------- TS06 ----------------

    @Test
    @DisplayName("TS6")
    void testTS6Comprehensive() {
        List<String> inputs = List.of(
            "2026-04-13T13:42:43.271025800+03:00",
            "2026-04-13T13:42:43.271025800+03:00Europe/Moscow",
            "2026-04-13T13:42:43.271025800+03:00[Europe/Moscow]",
            "2026-04-13T13:42:43.271025800Z",
            "2026-04-13T13:42:43.271025800+00:00",
            "2026-04-13T13:42:43.271025800-07:00",
            "2026-04-13T13:42:43.271025800+05:30",
            "2026-04-13T13:42:43.271025800[Europe/Moscow]",
            "2026-04-13 13:42:43.27102511+03:00",
            "2026-04-13T13:42:43.271025+03:00",
            "2026-04-13T13:42:43.271025000000+02:00",
            "2026-04-13T13:42:43.271025+0300",
            "2026-04-13T13:42:43.271025-0430",
            "2026-04-13T13:42:43.271025Z[UTC]",
            "2026-04-13T13:42:43.271025800+12:00",
            "2026-04-13T13:42:43.271025800-11:00",
            "2026-04-13T00:00:00.000000+00:00",
            "2026-04-13T23:59:59.999999999-01:00",
            "2026-04-13T13:42:43.271025800+03:45",
            "2026-04-13T13:42:43.271025800+04:00"
        );

        Map<String, String> expected = Map.ofEntries(
            Map.entry("2026-04-13T13:42:43.271025800+03:00", "2026-04-13 13:42:43.271025+03"),
            Map.entry("2026-04-13T13:42:43.271025800+03:00Europe/Moscow", "2026-04-13 13:42:43.271025+03"),
            Map.entry("2026-04-13T13:42:43.271025800+03:00[Europe/Moscow]", "2026-04-13 13:42:43.271025+03"),
            Map.entry("2026-04-13T13:42:43.271025800Z", "2026-04-13 13:42:43.271025+00"),
            Map.entry("2026-04-13T13:42:43.271025800+00:00", "2026-04-13 13:42:43.271025+00"),
            Map.entry("2026-04-13T13:42:43.271025800-07:00", "2026-04-13 13:42:43.271025-07"),
            Map.entry("2026-04-13T13:42:43.271025800+05:30", "2026-04-13 13:42:43.271025+05:30"),
            Map.entry("2026-04-13T13:42:43.271025800[Europe/Moscow]", "2026-04-13 13:42:43.271025+03")
        );

        StructType schema = createStructType(List.of(createStructField("raw", DataTypes.StringType, true)));
        List<Row> rows = inputs.stream().map(RowFactory::create).collect(Collectors.toList());
        Dataset<Row> df = spark.createDataFrame(rows, schema);
        Dataset<Row> result = df.withColumn("mutated", TS6.mutate("raw"));

        List<Row> collected = result.collectAsList();
        for (Row r : collected) {
            String input = r.getString(0);
            String actual = r.getString(1);
            assertEquals(expected.get(input), actual, "Ошибка для входного значения: " + input);
        }
    }

    // ---------------- TS09 ----------------

    @ParameterizedTest(name = "TS9 [{index}]: {0}")
    @MethodSource("provideTS9TestData")
    void testTS9_Comprehensive(String desc, String input, Object expected) {
        StructType schema = createStructType(List.of(createStructField("raw", DataTypes.StringType, true)));
        Dataset<Row> df = spark.createDataFrame(List.of(RowFactory.create(input)), schema);
        Dataset<Row> result = df.withColumn("mutated", TS9.mutate("raw"));

        if ("error".equals(expected)) {
            assertThrows(SparkException.class, result::collectAsList,
                "Ожидалась ошибка трансформации для: " + input);
        } else {
            Row row = result.collectAsList().get(0);
            if (expected == null) {
                assertTrue(row.isNullAt(1), "Ожидался null для: " + input);
            } else {
                assertEquals(expected, row.get(1), "Неверное значение для: " + input);
            }
        }
    }

    static Stream<Arguments> provideTS9TestData() {
        return Stream.of(
            Arguments.of("Простое число", "123", 123L),
            Arguments.of("Отрицательное", "-456", -456L),
            Arguments.of("Max Long", "9223372036854775807", 9223372036854775807L),
            Arguments.of("Min Long", "-9223372036854775808", -9223372036854775808L),
            Arguments.of("С пробелами", " 1000 ", 1000L),
            Arguments.of("Ноль", "0", 0L),
            Arguments.of("Ведущие нули", "000008", 8L),
            Arguments.of("Пустая строка", "", null),
            Arguments.of("Только пробелы", "   ", null),
            Arguments.of("Null", null, null),
            Arguments.of("Плюс в начале", "+50", 50L),
            Arguments.of("Переполнение Max + 1", "9223372036854775808", "error"),
            Arguments.of("Переполнение Min - 1", "-9223372036854775809", "error"),
            Arguments.of("Экстремально большое", "1000000000000000000000", "error"),
            Arguments.of("Дробное с точкой", "12.3", "error"),
            Arguments.of("Дробное с запятой", "12,3", "error"),
            Arguments.of("Текст", "abc", "error"),
            Arguments.of("Число с буквами", "123a", "error"),
            Arguments.of("Спецсимволы", "@#$", "error"),
            Arguments.of("Двойной минус", "--10", "error")
        );
    }

    // ---------------- TS10 ----------------

    @ParameterizedTest(name = "TS10 (Float to Decimal) [{index}]: {0}")
    @MethodSource("provideTS10TestData")
    void testTS10_FloatToDecimal(String desc, Object input, String expectedRegex) {
        StructType schema = createStructType(List.of(createStructField("raw", DataTypes.FloatType, true)));
        Dataset<Row> df = spark.createDataFrame(List.of(RowFactory.create(input)), schema);
        Dataset<Row> result = df.select(TS10.mutate("raw"));

        if ("error".equals(expectedRegex)) {
            assertThrows(SparkException.class, result::collectAsList,
                "Ожидалось исключение для значения: " + input);
        } else {
            Row row = result.collectAsList().get(0);
            if (expectedRegex == null) {
                assertTrue(row.isNullAt(0), "Ожидался null для: " + desc);
            } else {
                String actual = row.getDecimal(0).toPlainString();
                assertTrue(actual.matches(expectedRegex),
                    String.format("Результат [%s] не совпал с паттерном [%s] в тесте: %s", actual, expectedRegex, desc));
            }
        }
    }

    static Stream<Arguments> provideTS10TestData() {
        return Stream.of(
            Arguments.of("Целое 1", 1f, "1\\.0{12}"),
            Arguments.of("Целое 123", 123f, "123\\.0{12}"),
            Arguments.of("Большое целое", 12345678f, "12345678\\.0{12}"),
            Arguments.of("Отрицательное целое", -1000000f, "-1000000\\.0{12}"),
            Arguments.of("Ноль", 0.0f, "0\\.0{12}"),
            Arguments.of("Минус ноль", -0.0f, "0\\.0{12}"),
            Arguments.of("Целое", 100.0f, "100\\.0{12}"),
            Arguments.of("Отрицательное", -50.5f, "-50\\.5{11}"),
            Arguments.of("Дробное", 0.123456f, "0\\.1234560{6}"),
            Arguments.of("Округление (точность float)", 1.111111f, "1\\.1111110{6}"),
            Arguments.of("Дробное (предел float)", 0.1234567f, "0\\.1234567[0-5]"),
            Arguments.of("Экспонента +", 1.25E5f, "123000\\.0{12}"),
            Arguments.of("Экспонента -", 1.25E-4f, "0\\.0001230{6}"),
            Arguments.of("Экспонента большая", 1.0E20f, "100000000000000000000\\.0{12}"),
            Arguments.of("Null вход", null, null),
            Arguments.of("NaN", Float.NaN, "error"),
            Arguments.of("Positive Infinity", Float.POSITIVE_INFINITY, "error"),
            Arguments.of("Negative Infinity", Float.NEGATIVE_INFINITY, "error")
        );
    }

    // ---------------- TS11 ----------------

    @ParameterizedTest(name = "TS11 (Float to Double) [{index}]: {0}")
    @MethodSource("provideTS11TestData")
    void testTS11_FloatToDouble(String desc, Float input, Double expectedValue) {
        StructType schema = createStructType(List.of(createStructField("raw", DataTypes.FloatType, true)));
        Dataset<Row> df = spark.createDataFrame(List.of(RowFactory.create(input)), schema);
        Dataset<Row> result = df.select(TS11.mutate("raw"));

        Row row = result.collectAsList().get(0);

        if (input == null) {
            assertTrue(row.isNullAt(0));
        } else {
            double actual = row.getDouble(0);
            if (Double.isNaN(expectedValue)) {
                assertTrue(Double.isNaN(actual), "Ожидался NaN");
            } else {
                assertEquals(expectedValue, actual, 0.00001,
                    String.format("Ошибка в %s: ожидалось %s, получили %s", desc, expectedValue, actual));
                if (expectedValue == 0.0 && Double.compare(expectedValue, -0.0) == 0) {
                    assertEquals(0, Double.compare(actual, -0.0), "Ожидался именно -0.0");
                }
            }
        }
    }

    static Stream<Arguments> provideTS11TestData() {
        return Stream.of(
            Arguments.of("Целое число", 123f, 123.0),
            Arguments.of("Большое целое", 12345678f, 12345678.0),
            Arguments.of("Отрицательное целое", -100000f, -100000.0),
            Arguments.of("Дробное", 0.12345f, 0.12345),
            Arguments.of("Ноль", 0.0f, 0.0),
            Arguments.of("Минус ноль", -0.0f, -0.0),
            Arguments.of("Экспонента +", 1.111111E10f, 1.11111109552E10),
            Arguments.of("Экспонента -", 1.23E-4f, 0.0001230000013485551),
            Arguments.of("Чистая экспонента", 1.25E6f, 1250000.0),
            Arguments.of("Очень маленькое (Float MIN)", Float.MIN_VALUE, (double) Float.MIN_VALUE),
            Arguments.of("NaN", Float.NaN, Double.NaN),
            Arguments.of("Positive Infinity", Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY),
            Arguments.of("Negative Infinity", Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY),
            Arguments.of("Null вход", null, null)
        );
    }

    // ---------------- TS12 ----------------

    @ParameterizedTest(name = "TS12 (Double to Decimal) [{index}]: {0}")
    @MethodSource("provideTS12TestData")
    void testTS12_DoubleToDecimal(String desc, Double input, String expectedRegex) {
        StructType schema = createStructType(List.of(createStructField("raw", DataTypes.DoubleType, true)));
        Dataset<Row> df = spark.createDataFrame(List.of(RowFactory.create(input)), schema);
        Dataset<Row> result = df.select(TS12.mutate("raw"));

        if ("error".equals(expectedRegex)) {
            assertThrows(SparkException.class, result::collectAsList,
                "Ожидался сбой для значения: " + input);
        } else {
            Row row = result.collectAsList().get(0);
            if (expectedRegex == null) {
                assertTrue(row.isNullAt(0), "Ожидался null для: " + desc);
            } else {
                String actual = row.getDecimal(0).toPlainString();
                assertTrue(actual.matches(expectedRegex),
                    String.format("Результат [%s] не совпал с паттерном [%s] в тесте: %s", actual, expectedRegex, desc));
            }
        }
    }

    static Stream<Arguments> provideTS12TestData() {
        return Stream.of(
            Arguments.of("Целое положительное", 100.0, "100\\.0{12}"),
            Arguments.of("Целое отрицательное", -500.0, "-500\\.0{12}"),
            Arguments.of("Большое целое", 123456789012.0, "123456789012\\.0{12}"),
            Arguments.of("Дробное 12 знаков", 1.123456789012, "1\\.123456789012"),
            Arguments.of("Дробное с округлением", 0.123456789012345, "0\\.123456789012"),
            Arguments.of("Экспонента +", 1.111111E10, "11111110000\\.0{12}"),
            Arguments.of("Экспонента -", 1.23E-4, "0\\.0001230{6}"),
            Arguments.of("Экспонента большая", 1.0E20, "100000000000000000000\\.0{12}"),
            Arguments.of("Ноль", 0.0, "0\\.0{12}"),
            Arguments.of("Минус ноль", -0.0, "0\\.0{12}"),
            Arguments.of("Null вход", null, null),
            Arguments.of("NaN", Double.NaN, "error"),
            Arguments.of("Positive Infinity", Double.POSITIVE_INFINITY, "error"),
            Arguments.of("Negative Infinity", Double.NEGATIVE_INFINITY, "error")
        );
    }

    // ---------------- TS13 ----------------

    @ParameterizedTest(name = "TS13 [{index}]: {0}")
    @MethodSource("provideTS13TestData")
    void testTS13_DateTimeFormatting(String desc, String input, String expected) {
        StructType schema = createStructType(List.of(createStructField("raw", DataTypes.StringType, true)));
        Dataset<Row> df = spark.createDataFrame(List.of(RowFactory.create(input)), schema);
        Dataset<Row> result = df.withColumn("mutated", TS13.mutate("raw"));

        if ("error".equals(expected)) {
            assertThrows(SparkException.class, result::collectAsList);
        } else {
            Row row = result.collectAsList().get(0);
            if (expected == null) {
                assertTrue(row.isNullAt(1));
            } else {
                assertEquals(expected, row.getString(1));
            }
        }
    }

    static Stream<Arguments> provideTS13TestData() {
        return Stream.of(
            Arguments.of("Стандартный ISO +HH:mm", "2024-05-20T10:15:30.123456789+03:00", "2024-05-20 10:15:30.123456"),
            Arguments.of("UTC (Z)", "2024-05-20T10:15:30.123456789Z", "2024-05-20 10:15:30.123456"),
            Arguments.of("Смещение без двоеточия +HHmm", "2024-05-20T10:15:30.123456789+0300", "2024-05-20 10:15:30.123456"),
            Arguments.of("Отрицательное смещение", "2024-05-20T10:15:30.123456789-07:00", "2024-05-20 10:15:30.123456"),
            Arguments.of("Смещение с минутами +05:30", "2024-05-20T15:45:30.123456789+05:30", "2024-05-20 10:15:30.123456"),
            Arguments.of("Минимум наносекунд", "2024-05-20T10:15:30.000000001Z", "2024-05-20 10:15:30.000000"),
            Arguments.of("Полночь", "2024-01-01T00:00:00.000000000Z", "2024-01-01 00:00:00.000000"),
            Arguments.of("Конец дня", "2024-12-31T23:59:59.999999999Z", "2024-12-31 23:59:59.999999"),
            Arguments.of("С пробелами по краям", " 2024-05-20T10:15:30.123456789Z ", "2024-05-20 10:15:30.123456"),
            Arguments.of("Пустая строка", "", null),
            Arguments.of("Null", null, null),
            Arguments.of("Только пробелы", "   ", null),
            Arguments.of("Без смещения (ошибка паттерна XXXXX)", "2024-05-20T10:15:30.123456789", "error"),
            Arguments.of("Невалидная дата", "2024-13-20T10:15:30.123456789Z", "error"),
            Arguments.of("Невалидное время", "2024-05-20T25:15:30.123456789Z", "error"),
            Arguments.of("Разделитель пробел вместо T", "2024-05-20 10:15:30.123456789Z", "error"),
            Arguments.of("Мало знаков в nano", "2024-05-20T10:15:30.123Z", "error"),
            Arguments.of("Текст вместо даты", "not-a-date", "error"),
            Arguments.of("Смещение текстом", "2024-05-20T10:15:30.123456789MSK", "error")
        );
    }

    // ---------------- TS14 ----------------

    @ParameterizedTest(name = "TS14 (UTC) [{index}]: {0}")
    @MethodSource("provideTS14TestData")
    void testTS14_ToUtcFormatting(String desc, String input, String expected) {
        StructType schema = createStructType(List.of(createStructField("raw", DataTypes.StringType, true)));
        Dataset<Row> df = spark.createDataFrame(List.of(RowFactory.create(input)), schema);
        Dataset<Row> result = df.withColumn("mutated", TS14.mutate("raw"));

        if ("error".equals(expected)) {
            assertThrows(SparkException.class, result::collectAsList);
        } else {
            Row row = result.collectAsList().get(0);
            if (expected == null) {
                assertTrue(row.isNullAt(1));
            } else {
                assertEquals(expected, row.getString(1), "Ошибка в пересчете UTC для: " + input);
            }
        }
    }

    static Stream<Arguments> provideTS14TestData() {
        return Stream.of(
            Arguments.of("Стандарт +03:00 (МСК)", "2024-05-20T13:15:30.123456789+03:00", "2024-05-20 10:15:30.123456"),
            Arguments.of("UTC (Z)", "2024-05-20T10:15:30.123456789Z", "2024-05-20 10:15:30.123456"),
            Arguments.of("Без двоеточия +0300", "2024-05-20T13:15:30.123456789+0300", "2024-05-20 10:15:30.123456"),
            Arguments.of("Отрицательное -07:00", "2024-05-20T03:15:30.123456789-07:00", "2024-05-20 10:15:30.123456"),
            Arguments.of("Смещение с минутами +05:30", "2024-05-20T15:45:30.123456789+05:30", "2024-05-20 10:15:30.123456"),
            Arguments.of("Переход вперед (на след. день)", "2024-05-20T23:00:00.000000000-05:00", "2024-05-21 04:00:00.000000"),
            Arguments.of("Переход назад (на пред. день)", "2024-05-20T01:00:00.000000000+05:00", "2024-05-19 20:00:00.000000"),
            Arguments.of("Полночь UTC", "2024-01-01T00:00:00.000000000Z", "2024-01-01 00:00:00.000000"),
            Arguments.of("Полночь со смещением", "2024-01-01T00:00:00.000000000+02:00", "2023-12-31 22:00:00.000000"),
            Arguments.of("Конец года со смещением", "2024-12-31T23:59:59.999999999-01:00", "2025-01-01 00:59:59.999999"),
            Arguments.of("С пробелами", " 2024-05-20T10:15:30.123456789Z ", "2024-05-20 10:15:30.123456"),
            Arguments.of("Пустая строка", "", null),
            Arguments.of("Null", null, null),
            Arguments.of("Только пробелы", "   ", null),
            Arguments.of("Без смещения (обязательно для OffsetDateTime)", "2024-05-20T10:15:30.123456789", "error"),
            Arguments.of("Невалидная дата", "2024-13-20T10:15:30.123456789Z", "error"),
            Arguments.of("Разделитель пробел", "2024-05-20 10:15:30.123456789Z", "error"),
            Arguments.of("Мало знаков nano", "2024-05-20T10:15:30.123Z", "error"),
            Arguments.of("Текст", "not-a-date", "error"),
            Arguments.of("Смещение буквами", "2024-05-20T10:15:30.123456789MSK", "error"),
            Arguments.of("Неполное смещение", "2024-05-20T10:15:30.123456789+03", "error"),
            Arguments.of("Двойное смещение", "2024-05-20T10:15:30.123456789+03:00+01:00", "error")
        );
    }

    // ---------------- TS15 ----------------

    @ParameterizedTest(name = "TS15 (As-Is Zone) [{index}]: {0}")
    @MethodSource("provideTS15TestData")
    void testTS15_AsIsZoneFormatting(String desc, String input, String expected) {
        StructType schema = createStructType(List.of(createStructField("raw", DataTypes.StringType, true)));
        Dataset<Row> df = spark.createDataFrame(List.of(RowFactory.create(input)), schema);
        Dataset<Row> result = df.withColumn("mutated", TS15.mutate("raw"));

        if ("error".equals(expected)) {
            assertThrows(SparkException.class, result::collectAsList,
                "Ожидалась ошибка для некорректного формата зоны: " + input);
        } else {
            Row row = result.collectAsList().get(0);
            if (expected == null) {
                assertTrue(row.isNullAt(1));
            } else {
                assertEquals(expected, row.getString(1), "Неверное форматирование 'as-is' для: " + input);
            }
        }
    }

    static Stream<Arguments> provideTS15TestData() {
        return Stream.of(
            Arguments.of("Москва", "2024-05-20T13:15:30.123456789+03:00[Europe/Moscow]", "2024-05-20 13:15:30.123456"),
            Arguments.of("Лондон (BST)", "2024-05-20T10:15:30.123456789+01:00[Europe/London]", "2024-05-20 10:15:30.123456"),
            Arguments.of("Нью-Йорк (EDT)", "2024-05-20T08:00:00.000000000-04:00[America/New_York]", "2024-05-20 08:00:00.000000"),
            Arguments.of("Токио (JST)", "2024-05-20T20:00:00.999999999+09:00[Asia/Tokyo]", "2024-05-20 20:00:00.999999"),
            Arguments.of("UTC с зоной", "2024-05-20T10:00:00.000000000Z[UTC]", "2024-05-20 10:00:00.000000"),
            Arguments.of("Зона с коротким ID", "2024-05-20T10:00:00.000000000+03:00[GMT+03:00]", "2024-05-20 10:00:00.000000"),
            Arguments.of("Смещение с минутами", "2024-05-20T10:00:00.000000000+05:30[Asia/Kolkata]", "2024-05-20 10:00:00.000000"),
            Arguments.of("Зона с подчеркиванием", "2024-05-20T10:00:00.000000000+02:00[Europe/Kaliningrad]", "2024-05-20 10:00:00.000000"),
            Arguments.of("Зона с дефисом", "2024-05-20T10:00:00.000000000-03:00[America/Argentina/Buenos_Aires]", "2024-05-20 10:00:00.000000"),
            Arguments.of("Пробелы по краям", " 2024-05-20T10:15:30.123456789Z ", "2024-05-20 10:15:30.123456"),
            Arguments.of("Несоответствие оффсета и зоны - не ошибка", "2024-05-20T10:15:30.123456789+05:00[Europe/Moscow]", "2024-05-20 08:15:30.123456"),
            Arguments.of("Null", null, null),
            Arguments.of("Пусто", "", null),
            Arguments.of("Пробелы", "   ", null),
            Arguments.of("Нет скобок [ ]", "2024-05-20T10:15:30.123456789+03:00Europe/Moscow", "error"),
            Arguments.of("Нет зоны", "2024-05-20T10:15:30.123456789+03:00", "error"),
            Arguments.of("Невалидная зона", "2024-05-20T10:15:30.123456789+03:00[Mars/Base]", "error"),
            Arguments.of("Не закрыта скобка", "2024-05-20T10:15:30.123456789+03:00[Europe/Moscow", "error"),
            Arguments.of("Разделитель пробел", "2024-05-20 10:15:30.123456789+03:00[Europe/Moscow]", "error"),
            Arguments.of("Мало знаков nano", "2024-05-20T10:15:30.123+03:00[Europe/Moscow]", "error"),
            Arguments.of("Текст вместо даты", "any-random-string", "error"),
            Arguments.of("Только зона", "[Europe/Moscow]", "error"),
            Arguments.of("Зона перед оффсетом", "2024-05-20T10:15:30.123456789[Europe/Moscow]+03:00", "error")
        );
    }

    // ---------------- TS16 ----------------

    @ParameterizedTest(name = "TS16 (ZDT to UTC) [{index}]: {0}")
    @MethodSource("provideTS16TestData")
    void testTS16_ZonedDateTimeToUtc(String desc, String input, String expected) {
        StructType schema = createStructType(List.of(createStructField("raw", DataTypes.StringType, true)));
        Dataset<Row> df = spark.createDataFrame(List.of(RowFactory.create(input)), schema);
        Dataset<Row> result = df.withColumn("mutated", TS16.mutate("raw"));

        if ("error".equals(expected)) {
            assertThrows(SparkException.class, result::collectAsList,
                "Ожидалась ошибка для: " + input);
        } else {
            Row row = result.collectAsList().get(0);
            if (expected == null) {
                assertTrue(row.isNullAt(1), "Ожидался null для: " + input);
            } else {
                assertEquals(expected, row.getString(1), "Неверный пересчет UTC для: " + input);
            }
        }
    }

    static Stream<Arguments> provideTS16TestData() {
        return Stream.of(
            Arguments.of("Москва (+3)", "2024-05-20T13:00:00.123456789+03:00[Europe/Moscow]", "2024-05-20 10:00:00.123456"),
            Arguments.of("Нью-Йорк (-4)", "2024-05-20T06:00:00.123456789-04:00[America/New_York]", "2024-05-20 10:00:00.123456"),
            Arguments.of("UTC", "2024-05-20T10:00:00.123456789Z[UTC]", "2024-05-20 10:00:00.123456"),
            Arguments.of("Индия (+5:30)", "2024-05-20T15:30:00.123456789+05:30[Asia/Kolkata]", "2024-05-20 10:00:00.123456"),
            Arguments.of("Непал (+5:45)", "2024-05-20T15:45:00.123456789+05:45[Asia/Kathmandu]", "2024-05-20 10:00:00.123456"),
            Arguments.of("Смена года назад", "2024-01-01T01:00:00.000000000+03:00[Europe/Moscow]", "2023-12-31 22:00:00.000000"),
            Arguments.of("Смена года вперед", "2024-12-31T21:00:00.000000000-05:00[America/New_York]", "2025-01-01 02:00:00.000000"),
            Arguments.of("Високосный день", "2024-02-29T10:00:00.000000000Z[UTC]", "2024-02-29 10:00:00.000000"),
            Arguments.of("Лондон лето (+1)", "2024-07-01T11:00:00.000000000+01:00[Europe/London]", "2024-07-01 10:00:00.000000"),
            Arguments.of("Лондон зима (+0)", "2024-01-01T10:00:00.000000000Z[Europe/London]", "2024-01-01 10:00:00.000000")
        );
    }

    // ---------------- TS17 ----------------

    @ParameterizedTest(name = "TS17 (Date to string) [{index}]: {0}")
    @MethodSource("provideTS17TestData")
    void testTS17_DateToStringComprehensive(String desc, Date input, String expected) {
        StructType schema = createStructType(List.of(createStructField("raw", DataTypes.DateType, true)));
        Dataset<Row> df = spark.createDataFrame(List.of(RowFactory.create(input)), schema);
        Dataset<Row> result = df.withColumn("mutated", TS17.mutate("raw"));

        Row row = result.collectAsList().get(0);
        if (expected == null) {
            assertTrue(row.isNullAt(1), "Ожидался null для входного значения: " + input);
        } else {
            assertEquals(expected, row.getString(1), "Ошибка форматирования даты для: " + input);
        }
    }

    static Stream<Arguments> provideTS17TestData() {
        return Stream.of(
            Arguments.of("Стандартная дата", Date.valueOf("2024-05-20"), "2024-05-20 00:00:00.000000"),
            Arguments.of("Первый день года", Date.valueOf("2024-01-01"), "2024-01-01 00:00:00.000000"),
            Arguments.of("Последний день года", Date.valueOf("2024-12-31"), "2024-12-31 00:00:00.000000"),
            Arguments.of("Високосный день", Date.valueOf("2024-02-29"), "2024-02-29 00:00:00.000000"),
            Arguments.of("Эпоха Unix (Zero)", Date.valueOf("1970-01-01"), "1970-01-01 00:00:00.000000"),
            Arguments.of("Историческая дата", Date.valueOf("1900-01-01"), "1900-01-01 00:00:00.000000"),
            Arguments.of("Будущая дата", Date.valueOf("2100-12-31"), "2100-12-31 00:00:00.000000"),
            Arguments.of("Null значение", null, null)
        );
    }

    // ---------------- TS18 ----------------

    @ParameterizedTest(name = "TS18 (LocalTime) [{index}]: {0}")
    @MethodSource("provideTS18TestData")
    void testTS18_LocalTime(String desc, String input, String expected) {
        StructType schema = createStructType(List.of(createStructField("raw", DataTypes.StringType, true)));
        Dataset<Row> df = spark.createDataFrame(List.of(RowFactory.create(input)), schema);
        Dataset<Row> result = df.withColumn("mutated", TS18.mutate("raw"));

        if ("error".equals(expected)) {
            assertThrows(SparkException.class, result::collectAsList);
        } else {
            Row row = result.collectAsList().get(0);
            if (expected == null) {
                assertTrue(row.isNullAt(1));
            } else {
                assertEquals(expected, row.getString(1));
            }
        }
    }

    static Stream<Arguments> provideTS18TestData() {
        return Stream.of(
            Arguments.of("Все наносекунды", "13:42:43.123456789", "13:42:43.123456789"),
            Arguments.of("Только секунды", "13:42:43.000000000", "13:42:43"),
            Arguments.of("Полночь", "00:00:00.000000000", "00:00"),
            Arguments.of("Микросекунды", "10:00:00.123456000", "10:00:00.123456"),
            Arguments.of("Граничное время", "23:59:59.999999999", "23:59:59.999999999"),
            Arguments.of("С пробелами", " 08:30:00.000000000 ", "08:30"),
            Arguments.of("Невалидные часы", "25:00:00.000000000", "error"),
            Arguments.of("Мало знаков nano", "13:42:43.123", "error"),
            Arguments.of("Есть дата (лишнее)", "2024-05-20T13:42:43.123456789", "error"),
            Arguments.of("Null", null, null),
            Arguments.of("Пусто", "", null)
        );
    }

    // ---------------- TS19 ----------------

    @ParameterizedTest(name = "TS19 (Colon Process) [{index}]: {0}")
    @MethodSource("provideTS19TestData")
    void testTS19_ColonProcessing(String desc, String input, String expected) {
        StructType schema = createStructType(List.of(createStructField("raw", DataTypes.StringType, true)));
        Dataset<Row> df = spark.createDataFrame(List.of(RowFactory.create(input)), schema);
        Dataset<Row> result = df.withColumn("mutated", TS19.mutate("raw"));

        if ("error".equals(expected)) {
            assertThrows(SparkException.class, result::collectAsList);
        } else {
            Row row = result.collectAsList().get(0);
            if (expected == null) {
                assertTrue(row.isNullAt(1));
            } else {
                assertEquals(expected, row.getString(1));
            }
        }
    }

    static Stream<Arguments> provideTS19TestData() {
        return Stream.of(
            Arguments.of("Успешная замена", "part.one.text:part.two.text", "part one text:part two text"),
            Arguments.of("Без точек в первой части", "partone:part.two", "partone:part two"),
            Arguments.of("Точка во второй части остается", "a.b:c.d", "a.b:c.d"),
            Arguments.of("Пустая первая часть", ":part2", ":part2"),
            Arguments.of("Пустая вторая часть", "part1.:", "part1.:"),
            Arguments.of("Null", null, null),
            Arguments.of("Пустая строка", "", ""),
            Arguments.of("Нет двоеточия", "part.one.no.colon", "error"),
            Arguments.of("Два двоеточия", "part.one:part.two:part.three", "error"),
            Arguments.of("Много двоеточий", ":::", "error")
        );
    }

    // ---------------- TS20 ----------------

    @ParameterizedTest(name = "TS20 (Json Comprehensive) [{index}]: {0}")
    @MethodSource("provideTS20TestData")
    void testTS20_JsonComprehensive(String desc, String input, String expectedRegex) {
        StructType schema = createStructType(List.of(createStructField("raw", DataTypes.StringType, true)));
        Dataset<Row> df = spark.createDataFrame(List.of(RowFactory.create(input)), schema);
        Dataset<Row> result = df.withColumn("mutated", TS20.mutate("raw"));

        if ("error".equals(expectedRegex)) {
            assertThrows(SparkException.class, result::collectAsList);
        } else {
            Row row = result.collectAsList().get(0);
            if (expectedRegex == null) {
                assertTrue(row.isNullAt(1));
            } else {
                String actual = row.getString(1);
                assertTrue(actual.matches(expectedRegex),
                    String.format("Результат [%s] не совпал с паттерном [%s]", actual, expectedRegex));
            }
        }
    }

    static Stream<Arguments> provideTS20TestData() {
        return Stream.of(
            Arguments.of("Один элемент", "[{\"key\":\"k1\",\"type\":\"TypeA\"}]", "k1:TypeA\\|\\d+:I"),
            Arguments.of("Два элемента", "[{\"key\":\"k1\",\"type\":\"TypeA\"},{\"key\":\"k2\",\"type\":\"TypeB\"}]", "k1:TypeA\\|\\d+:I;k2:TypeB\\|\\d+:I"),
            Arguments.of("Пустой массив", "[]", ""),
            Arguments.of("Отсутствующие ключи", "[{\"other\":\"val\"}]", ":\\d+:I"),
            Arguments.of("Частичные ключи", "[{\"key\":\"k1\"}]", "k1:\\d+:I"),
            Arguments.of("Доп. поля (игнорируются)", "[{\"key\":\"k1\",\"type\":\"T1\",\"extra\":\"ignore\"}]", "k1:T1\\|\\d+:I"),
            Arguments.of("Много элементов", "[{\"key\":\"1\"},{\"key\":\"2\"},{\"key\":\"3\"}]", "1:\\d+:I;2:\\d+:I;3:\\d+:I"),
            Arguments.of("Null вход", null, null),
            Arguments.of("Пустая строка", "", ""),
            Arguments.of("Строка с пробелами", "   ", ""),
            Arguments.of("Не массив (объект)", "{\"key\":\"k1\"}", "error"),
            Arguments.of("Невалидный JSON", "[{\"key\":\"k1\"", "error"),
            Arguments.of("Просто текст", "not a json", "error"),
            Arguments.of("Число вместо JSON", "123", "error")
        );
    }

    // ---------------- TS21 ----------------

    @ParameterizedTest(name = "TS21 (JSON Array to String) [{index}]: {0}")
    @MethodSource("provideTS21TestData")
    void testTS21_JsonArrayJoin(String desc, String input, String expected) {
        StructType schema = createStructType(List.of(createStructField("raw", DataTypes.StringType, true)));
        Dataset<Row> df = spark.createDataFrame(List.of(RowFactory.create(input)), schema);
        Dataset<Row> result = df.select(TS21.mutate("raw"));

        Row row = result.collectAsList().get(0);
        if (expected == null) {
            assertTrue(row.isNullAt(0));
        } else {
            assertEquals(expected, row.getString(0));
        }
    }

    static Stream<Arguments> provideTS21TestData() {
        return Stream.of(
            Arguments.of("Стандартный массив", "[\"123\", \"12345\"]", "123|12345"),
            Arguments.of("Массив с null", "[\"123\", \"12345\", null]", "123|12345|null"),
            Arguments.of("Массив только из null", "[null, null]", "null|null"),
            Arguments.of("Пустой массив", "[]", ""),
            Arguments.of("Один элемент", "[\"single\"]", "single"),
            Arguments.of("Null на входе", null, null)
        );
    }
}