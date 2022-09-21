
CREATE OR REPLACE VIEW generate_series_16
AS SELECT 0 generate_series from SYSIBM.SYSDUMMY1 UNION ALL SELECT 1 from SYSIBM.SYSDUMMY1 UNION ALL SELECT 2 from SYSIBM.SYSDUMMY1  UNION ALL
   SELECT 3 from SYSIBM.SYSDUMMY1  UNION ALL SELECT 4  from SYSIBM.SYSDUMMY1 UNION ALL SELECT 5 from SYSIBM.SYSDUMMY1  UNION ALL
   SELECT 6  from SYSIBM.SYSDUMMY1 UNION ALL SELECT 7  from SYSIBM.SYSDUMMY1 UNION ALL SELECT 8 from SYSIBM.SYSDUMMY1 UNION ALL
   SELECT 9  from SYSIBM.SYSDUMMY1 UNION ALL SELECT 10 from SYSIBM.SYSDUMMY1 UNION ALL SELECT 11 from SYSIBM.SYSDUMMY1 UNION ALL
   SELECT 12 from SYSIBM.SYSDUMMY1 UNION ALL SELECT 13 from SYSIBM.SYSDUMMY1 UNION ALL SELECT 14 from SYSIBM.SYSDUMMY1 UNION ALL
   SELECT 15 from SYSIBM.SYSDUMMY1;

CREATE OR REPLACE VIEW generate_series_256
AS SELECT ( hi.generate_series * 16 + lo.generate_series ) AS generate_series
   FROM generate_series_16 lo, generate_series_16 hi;

CREATE OR REPLACE VIEW generate_series_4k
AS SELECT ( hi.generate_series * 256 + lo.generate_series ) AS generate_series
   FROM generate_series_256 lo, generate_series_16 hi;

-- CREATE OR REPLACE FUNCTION random_string()
--     RETURNS VARCHAR(19)
-- BEGIN
--     RETURN TRANSLATE ( CHAR(BIGINT(RAND() * 10000000000000000 )), 'abcdefghi', '1234567890' );
-- END;

create table t_source (
    /*Exact Numerics*/
                          C_INTEGER INTEGER not null,
                          C_SMALLINT SMALLINT,
                          C_BIGINT NUMERIC(19)	,
                          C_NUMERIC NUMERIC(16,10),
                          C_DECIMAL DECIMAL(16,10),
    /*Approximate Numerics:*/
                          C_REAL REAL,
                          C_DOUBLE_PRECISION DOUBLE PRECISION,
                          C_FLOAT FLOAT,
    /*Binary Strings:*/
                          C_BINARY binary(35),
                          C_BINARY_VAR varbinary(255),
                          C_BINARY_LOB BLOB,
    /*Boolean:*/
                          C_BOOLEAN CHAR(1),
    /*Character Strings:*/
                          C_CHARACTER CHAR(35),
                          C_CHARACTER_VAR VARCHAR(255),
                          C_CHARACTER_LOB CLOB,
                          C_NATIONAL_CHARACTER nchar(35),
                          C_NATIONAL_CHARACTER_VAR nvarchar(255),
    /*Datetimes:*/
                          C_DATE DATE,
    --C_TIME_WITHOUT_TIMEZONE TIME,/*not supported*/
                          C_TIMESTAMP_WITHOUT_TIMEZONE TIMESTAMP,
    --C_TIME_WITH_TIMEZONE TIMESTAMP, /*not supported*/
                          C_TIMESTAMP_WITH_TIMEZONE TIMESTAMP,
    /*Intervals:*/
    -- C_INTERVAL_DAY INTERVAL DAY TO SECOND, /*not supported*/
    -- C_INTERVAL_YEAR INTERVAL YEAR TO MONTH, /*not supported*/
    --         /*Collection Types:*/
    --         ARRAY /*not supported*/
    --         MULTISET /*not supported*/
    --         /*Other Types:*/
    --         ROW /*not supported*/
    -- C_XML XMLType, /*not supported*/
    --          JSON /*not supported*/
                          CONSTRAINT c_integer_pk PRIMARY KEY (C_INTEGER)
);

insert into t_source (C_INTEGER,
                      C_SMALLINT,
                      C_BIGINT,
                      C_NUMERIC,
                      C_DECIMAL,
                      C_REAL,
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
                      C_TIMESTAMP_WITHOUT_TIMEZONE,
                      C_TIMESTAMP_WITH_TIMEZONE
)
select generate_series,
       CAST(generate_series * rand() as integer)           as C_SMALLINT,
       CAST(generate_series * rand() as integer)           as C_BIGINT,
       CAST(generate_series * rand() as decimal(16, 10))   as C_NUMERIC,
       CAST(generate_series * rand() as decimal(16, 10))   as C_DECIMAL,
       CAST(generate_series * rand() as decimal(16, 10))   as C_REAL,
       CAST(generate_series * rand() as decimal(16, 10))   as C_DOUBLE_PRECISION,
       CAST(generate_series * rand() as decimal(16, 10))   as C_FLOAT,
       binary(to_char(generate_series * rand()), 35)       as C_BINARY,
       binary(to_char(generate_series * rand()), 255)      as C_BINARY_VAR,
       binary(to_char(generate_series * rand()), 255)      as C_BINARY_LOB,
       '1',
       'abcdefghi'                                     as C_CHAR,
       'abcdefghi'                                     as C_VARCHAR,
       'abcdefghi'                                     as C_VARCHAR_LOB,
       'abcdefghi'                                     as C_NATIONAL_CHARACTER,
       'abcdefghi'                                     as C_NATIONAL_CHARACTER_VAR,
       current_date                                        as C_DATE,
       current_timestamp                                   as C_TIME_WITHOUT_TIMEZONE,
       TIMEZONE(current_timestamp, 'Europe/Madrid', 'UTC') as C_TIMESTAMP_WITH_TIMEZONE
from generate_series_4k;