drop table if exists t_sink;
create table t_sink
(
    /*Exact Numerics*/
    C_INTEGER                    INTEGER ,
    C_SMALLINT                   SMALLINT,
    C_BIGINT                     BIGINT,
    C_NUMERIC                    NUMERIC(65, 30),
    C_DECIMAL                    DECIMAL(65, 30),
    /*Approximate Numerics:*/
    C_REAL                       REAL,
    C_DOUBLE_PRECISION           DOUBLE PRECISION,
    C_FLOAT                      FLOAT,
    /*Binary Strings:*/
    C_BINARY                     BINARY(35),
    C_BINARY_VAR                 VARBINARY(255),
    C_BINARY_LOB                 BLOB,
    /*Boolean:*/
    C_BOOLEAN                    BOOLEAN,
    /*Character Strings:*/
    C_CHARACTER                  CHAR(35),
    C_CHARACTER_VAR              VARCHAR(255),
    C_CHARACTER_LOB              TEXT,
    C_NATIONAL_CHARACTER         NATIONAL CHARACTER(35),
    C_NATIONAL_CHARACTER_VAR     NVARCHAR(255),
    /*Datetimes:*/
    C_DATE                       DATE,
    C_TIME_WITHOUT_TIMEZONE      TIME,
    C_TIMESTAMP_WITHOUT_TIMEZONE TIMESTAMP,
    C_TIME_WITH_TIMEZONE         TIME,
    C_TIMESTAMP_WITH_TIMEZONE    TIMESTAMP,
    /*Intervals:*/
    C_INTERVAL_DAY               text/*not supported*/,
    C_INTERVAL_YEAR              text/*not supported*/,
    /*Collection Types:*/
    C_ARRAY                      text/*not supported*/,
    C_MULTIDIMENSIONAL_ARRAY     text/*not supported*/,
    C_MULTISET                   text/*not supported*/,
    /*Other Types:*/
    C_XML                        text/*not supported*/,
    C_JSON                       text/*not supported*/,
    PRIMARY KEY (C_INTEGER)
);
