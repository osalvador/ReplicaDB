
CREATE OR REPLACE VIEW generate_series_16
AS SELECT 0 generate_series UNION ALL SELECT 1  UNION ALL SELECT 2  UNION ALL
   SELECT 3   UNION ALL SELECT 4  UNION ALL SELECT 5  UNION ALL
   SELECT 6   UNION ALL SELECT 7  UNION ALL SELECT 8  UNION ALL
   SELECT 9   UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL
   SELECT 12  UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL
   SELECT 15;

CREATE OR REPLACE VIEW generate_series_256
AS SELECT ( ( hi.generate_series << 4 ) | lo.generate_series ) AS generate_series
   FROM generate_series_16 lo, generate_series_16 hi;

CREATE OR REPLACE VIEW generate_series_4k
AS SELECT ( ( hi.generate_series << 8 ) | lo.generate_series ) AS generate_series
   FROM generate_series_256 lo, generate_series_16 hi;

create table t_source (
    /*Exact Numerics*/
    C_INTEGER INTEGER AUTO_INCREMENT,
    C_SMALLINT SMALLINT,
    C_BIGINT BIGINT,
    C_NUMERIC NUMERIC(65,30),
    C_DECIMAL DECIMAL(65,30),
    /*Approximate Numerics:*/
    C_REAL REAL,
    C_DOUBLE_PRECISION DOUBLE PRECISION,
    C_FLOAT FLOAT,
    /*Binary Strings:*/
    C_BINARY BINARY(35),
    C_BINARY_VAR VARBINARY(255),
    C_BINARY_LOB BLOB,
    /*Boolean:*/
    C_BOOLEAN BOOLEAN,
    /*Character Strings:*/
    C_CHARACTER CHAR(35),
    C_CHARACTER_VAR VARCHAR(255),
    C_CHARACTER_LOB TEXT,
    C_NATIONAL_CHARACTER NATIONAL CHARACTER(35),
    C_NATIONAL_CHARACTER_VAR NVARCHAR(255),
    /*Datetimes:*/
    C_DATE DATE,
    C_TIME_WITHOUT_TIMEZONE TIME,
    C_TIMESTAMP_WITHOUT_TIMEZONE TIMESTAMP,
    C_TIME_WITH_TIMEZONE TIME,
    C_TIMESTAMP_WITH_TIMEZONE TIMESTAMP,
    --         /*Intervals:*/
    --         INTERVAL DAY /*not supported*/
    --         INTERVAL YEAR /*not supported*/
    --         /*Collection Types:*/
    --         ARRAY /*not supported*/
    --         MULTISET /*not supported*/
    --         /*Other Types:*/
    --         ROW /*not supported*/
    --         XML /*not supported*/
    --          JSON /*not supported*/
    PRIMARY KEY (C_INTEGER)
);

insert into t_source (
    /*C_INTEGER, auto incremented*/
    C_SMALLINT,
    C_BIGINT,
    C_NUMERIC,
    C_DECIMAL,
    C_REAL ,
    C_DOUBLE_PRECISION,
    C_FLOAT,
    C_BINARY,
    C_BINARY_VAR,
    C_BINARY_LOB,
    C_BOOLEAN,
    C_CHARACTER,
    C_CHARACTER_VAR,
    C_CHARACTER_LOB,
    C_NATIONAL_CHARACTER,
    C_NATIONAL_CHARACTER_VAR,
    C_DATE,
    C_TIME_WITHOUT_TIMEZONE,
    C_TIMESTAMP_WITHOUT_TIMEZONE,
    C_TIME_WITH_TIMEZONE ,
    C_TIMESTAMP_WITH_TIMEZONE
)
select
    CAST(generate_series * rand() as signed) as C_SMALLINT,
    CAST(generate_series * rand() as signed) as C_BIGINT,
    CAST(generate_series * rand() as decimal(65, 30)) as C_NUMERIC,
    CAST(generate_series * rand() as decimal(65, 30)) as C_DECIMAL,
    CAST(generate_series * rand() as decimal(25, 10)) as C_REAL,
    CAST(generate_series * rand() as decimal(25, 10)) as C_DOUBLE_PRECISION,
    CAST(generate_series * rand() as decimal(25, 10)) as C_FLOAT,
    BINARY(md5(rand())) as C_BINARY,
    BINARY(md5(rand())) as C_BINARY_VAR,
    BINARY(md5(rand())) as C_BINARY_LOB,
    TRUE,
    md5(rand()) as C_CHAR,
    md5(rand()) as C_VARCHAR,
    md5(rand()) as C_VARCHAR_LOB,
    CONVERT(md5(rand()) using utf8) as C_NATIONAL_CHARACTER,
    CONVERT(md5(rand()) using utf8) as C_NATIONAL_CHARACTER_VAR,
    current_date as C_DATE,
    current_time as C_TIME_WITHOUT_TIMEZONE,
    current_timestamp as C_TIMESTAMP_WITHOUT_TIMEZONE,
    CONVERT_TZ(current_time, '+00:00', '+02:00' ) as C_TIME_WITH_TIMEZONE,
    CONVERT_TZ(current_timestamp, '+00:00', '+02:00' ) as C_TIMESTAMP_WITH_TIMEZONE
from generate_series_4k;
