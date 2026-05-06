USE transform_demo;

DROP VIEW IF EXISTS transformed_view;

CREATE VIEW transformed_view AS
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

FROM source_input