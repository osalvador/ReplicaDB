create table t_sink
(
/*Exact Numerics*/
    C_INTEGER                    NUMERIC(11,0) not null,
    C_SMALLINT                   NUMERIC (5,0),
    C_BIGINT                     NUMERIC (20,0),
    C_NUMERIC                    NUMERIC(30, 15),
    C_DECIMAL                    DECIMAL(30, 15),
    /*Approximate Numerics:*/
    C_REAL                       DECIMAL(30, 15),
    C_DOUBLE_PRECISION           DECIMAL(30, 15),
    C_FLOAT                      DECIMAL(30, 15),
    /*Binary Strings:*/
    C_BINARY                     BLOB,
    C_BINARY_VAR                 BLOB,
    C_BINARY_LOB                 BLOB,
    /*Boolean:*/
    C_BOOLEAN                    CHAR(1),
    /*Character Strings:*/
    C_CHARACTER                  CHAR(35),
    C_CHARACTER_VAR              varchar(255),
    C_CHARACTER_LOB              CLOB,
    C_NATIONAL_CHARACTER         NCHAR(35),
    C_NATIONAL_CHARACTER_VAR     NVARCHAR(255),
    /*Datetimes:*/
    C_DATE                       DATE,
    C_TIME_WITHOUT_TIMEZONE      varchar(100) /*not supported*/,
    C_TIMESTAMP_WITHOUT_TIMEZONE TIMESTAMP,
    C_TIME_WITH_TIMEZONE         varchar(100) /*not supported*/,
    C_TIMESTAMP_WITH_TIMEZONE    TIMESTAMP,
    /*Intervals:*/
    C_INTERVAL_DAY               CLOB/*not supported*/,
    C_INTERVAL_YEAR              CLOB/*not supported*/,
    /*Collection Types:*/
    C_ARRAY                      CLOB/*not supported*/,
    C_MULTIDIMENSIONAL_ARRAY     CLOB/*not supported*/,
    C_MULTISET                   CLOB /*not supported*/,
    /*Other Types:*/
    --         ROW /*not supported*/
    C_XML                        CLOB,
    C_JSON                       CLOB /* supported > 11g */,
    PRIMARY KEY (C_INTEGER)
);