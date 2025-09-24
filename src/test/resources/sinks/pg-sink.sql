create table if not exists t_sink
(
    /*Exact Numerics*/
    C_INTEGER                    integer,
    C_SMALLINT                   smallint,
    C_BIGINT                     bigint,
    C_NUMERIC                    numeric,
    C_DECIMAL                    decimal,
    /*Approximate Numerics:*/
    C_REAL                       real,
    C_DOUBLE_PRECISION           double precision,
    C_FLOAT                      float,
    /*Binary Strings:*/
    C_BINARY                     BYTEA,
    C_BINARY_VAR                 BYTEA,
    C_BINARY_LOB                 BYTEA,
    /*Boolean:*/
    C_BOOLEAN                    boolean,
    /*Character Strings:*/
    C_CHARACTER                  CHAR(35),
    C_CHARACTER_VAR              VARCHAR(255),
    C_CHARACTER_LOB              TEXT,
    C_NATIONAL_CHARACTER         CHAR(35),
    C_NATIONAL_CHARACTER_VAR     VARCHAR(255),
    /*Datetimes:*/
    C_DATE                       DATE,
    C_TIME_WITHOUT_TIMEZONE      time without time zone,
    C_TIMESTAMP_WITHOUT_TIMEZONE TIMESTAMP without time zone,
    C_TIME_WITH_TIMEZONE         time with time zone,
    C_TIMESTAMP_WITH_TIMEZONE    TIMESTAMP with time zone,
    /*Intervals:*/
    C_INTERVAL_DAY               INTERVAL,
    C_INTERVAL_YEAR              INTERVAL,
    /*Collection Types:*/
    C_ARRAY                      text[],
    C_MULTISET                   text/*not supported*/,
    /*Other Types:*/
    --         ROW /*not supported*/
    C_XML                        xml,
    C_JSON                       jsonb,
    PRIMARY KEY (C_INTEGER)
);