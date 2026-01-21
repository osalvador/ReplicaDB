DROP TABLE IF EXISTS t_sink;
CREATE TABLE t_sink
(
    /*Exact Numerics*/
    C_INTEGER                    INTEGER PRIMARY KEY,
    C_SMALLINT                   SMALLINT,
    C_BIGINT                     BIGINT,
    C_NUMERIC                    DECIMAL(38, 10),
    C_DECIMAL                    DECIMAL(38, 10),
    /*Approximate Numerics:*/
    C_REAL                       REAL,
    C_DOUBLE_PRECISION           DOUBLE,
    C_FLOAT                      FLOAT,
    /*Boolean:*/
    C_BOOLEAN                    BOOLEAN,
    /*Character Strings:*/
    C_CHARACTER                  VARCHAR(35),
    C_CHARACTER_VAR              VARCHAR(255),
    /*Datetimes:*/
    C_DATE                       DATE,
    C_TIME_WITHOUT_TIMEZONE      TIME,
    C_TIMESTAMP_WITHOUT_TIMEZONE TIMESTAMP
);
