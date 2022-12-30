
CREATE OR REPLACE VIEW generate_series_16
AS SELECT 0 generate_series from dual UNION ALL SELECT 1 from dual UNION ALL SELECT 2 from dual  UNION ALL
   SELECT 3 from dual  UNION ALL SELECT 4  from dual UNION ALL SELECT 5 from dual  UNION ALL
   SELECT 6  from dual UNION ALL SELECT 7  from dual UNION ALL SELECT 8 from dual UNION ALL
   SELECT 9  from dual UNION ALL SELECT 10 from dual UNION ALL SELECT 11 from dual UNION ALL
   SELECT 12 from dual UNION ALL SELECT 13 from dual UNION ALL SELECT 14 from dual UNION ALL
   SELECT 15 from dual;

CREATE OR REPLACE VIEW generate_series_256
AS SELECT ( hi.generate_series * 16 + lo.generate_series ) AS generate_series
   FROM generate_series_16 lo, generate_series_16 hi;

CREATE OR REPLACE VIEW generate_series_4k
AS SELECT ( hi.generate_series * 256 + lo.generate_series ) AS generate_series
   FROM generate_series_256 lo, generate_series_16 hi;

create table t_source (
    /*Exact Numerics*/
    C_INTEGER INTEGER,
    C_SMALLINT SMALLINT,
    C_BIGINT NUMBER(19)	,
    C_NUMERIC NUMERIC(34,25),
    C_DECIMAL DECIMAL(34,25),
    /*Approximate Numerics:*/
    C_REAL REAL,
    C_DOUBLE_PRECISION DOUBLE PRECISION,
    C_FLOAT FLOAT,
    /*Binary Strings:*/
    C_BINARY RAW(35),
    C_BINARY_VAR RAW(255),
    C_BINARY_LOB BLOB,
    /*Boolean:*/
    C_BOOLEAN CHAR(1),
    /*Character Strings:*/
    C_CHARACTER CHAR(35),
    C_CHARACTER_VAR VARCHAR(255),
    C_CHARACTER_LOB CLOB,
    C_NATIONAL_CHARACTER NATIONAL CHARACTER(35),
    C_NATIONAL_CHARACTER_VAR NATIONAL CHARACTER(255),
    /*Datetimes:*/
    C_DATE DATE,
    --C_TIME_WITHOUT_TIMEZONE TIME,/*not supported*/
    C_TIMESTAMP_WITHOUT_TIMEZONE TIMESTAMP,
    --C_TIME_WITH_TIMEZONE TIMESTAMP, /*not supported*/
    C_TIMESTAMP_WITH_TIMEZONE TIMESTAMP,
    /*Intervals:*/
    C_INTERVAL_DAY INTERVAL DAY TO SECOND,
    C_INTERVAL_YEAR INTERVAL YEAR TO MONTH,
    --         /*Collection Types:*/
    --         ARRAY /*not supported*/
    --         MULTISET /*not supported*/
    --         /*Other Types:*/
    --         ROW /*not supported*/
    C_XML XMLType,
    --          JSON /*not supported*/
    CONSTRAINT c_integer_pk PRIMARY KEY (C_INTEGER)
);

insert into t_source (
    C_INTEGER,
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
    C_TIMESTAMP_WITHOUT_TIMEZONE,
    C_TIMESTAMP_WITH_TIMEZONE,
    C_INTERVAL_DAY,
    C_INTERVAL_YEAR,
    C_XML
)
select
    generate_series,
    CAST(generate_series * DBMS_RANDOM.NORMAL() as integer) as C_SMALLINT,
    CAST(generate_series * DBMS_RANDOM.NORMAL() as integer) as C_BIGINT,
    CAST(generate_series * DBMS_RANDOM.NORMAL() as decimal(34, 25)) as C_NUMERIC,
    CAST(generate_series * DBMS_RANDOM.NORMAL() as decimal(34, 25)) as C_DECIMAL,
    CAST(generate_series * DBMS_RANDOM.NORMAL() as decimal(34, 10)) as C_REAL,
    CAST(generate_series * DBMS_RANDOM.NORMAL() as decimal(34, 10)) as C_DOUBLE_PRECISION,
    CAST(generate_series * DBMS_RANDOM.NORMAL() as decimal(34, 10)) as C_FLOAT,
    UTL_RAW.cast_to_raw(substr(generate_series * DBMS_RANDOM.NORMAL(),35)) as C_BINARY,
    UTL_RAW.cast_to_raw(generate_series * DBMS_RANDOM.NORMAL()) as C_BINARY_VAR,
    UTL_RAW.cast_to_raw(generate_series * DBMS_RANDOM.NORMAL()) as C_BINARY_LOB,
    '1',
    TRIM(to_char(generate_series, 'XXXXXX')) as C_CHAR,
    TRIM(to_char(generate_series, 'XXXXXX'))as C_VARCHAR,
    TRIM(to_char(generate_series, 'XXXXXX')) as C_VARCHAR_LOB,
    CONVERT(TRIM(to_char(generate_series, 'XXXXXX')),'us7ascii','AL32UTF8') as C_NATIONAL_CHARACTER,
    CONVERT(TRIM(to_char(generate_series, 'XXXXXX')),'us7ascii','AL32UTF8')  as C_NATIONAL_CHARACTER_VAR,
    sysdate as C_DATE,
    systimestamp as C_TIME_WITHOUT_TIMEZONE,
    systimestamp AT TIME ZONE 'Europe/Madrid' as C_TIMESTAMP_WITH_TIMEZONE,
    INTERVAL '5 10:09:18' DAY TO SECOND C_INTERVAL_DAY,
    INTERVAL '12-2' YEAR TO MONTH C_INTERVAL_YEAR,
    XMLTYPE('<xml>hi</xml>') as C_XML
from generate_series_4k;
