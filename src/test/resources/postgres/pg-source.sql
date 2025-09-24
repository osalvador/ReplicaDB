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

create table if not exists t_source
(
    /*Exact Numerics*/
    C_INTEGER                    serial,
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
    C_MULTIDIMENSIONAL_ARRAY     int[][],
    C_MULTISET                   text/*not supported*/,
    /*Other Types:*/
    C_XML                        xml,
    C_JSON                       jsonb,
    PRIMARY KEY (C_INTEGER)
);

truncate table t_source;;

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
    C_TIMESTAMP_WITH_TIMEZONE,
    C_ARRAY,
    C_MULTIDIMENSIONAL_ARRAY,
    C_XML,
    C_JSON
)
select
    (generate_series * random())::integer as C_SMALLINT,
    (generate_series * random())::bigint as C_BIGINT,
    (generate_series * random())::numeric as C_NUMERIC,
    (generate_series * random())::decimal as C_DECIMAL,
    (generate_series * random())::real as C_REAL,
    (generate_series * random())::double precision as C_DOUBLE_PRECISION,
    (generate_series * random())::float as C_FLOAT,
    E'\\xDEADBEEF' as C_BINARY,
    E'\\xDEADBEEF' as C_BINARY_VAR,
    E'\\xDEADBEEF' as C_BINARY_LOB,
    TRUE,
    MD5((generate_series * random())::text) as C_CHAR,
    MD5((generate_series * random())::text) as C_VARCHAR,
    MD5((generate_series * random())::text) as C_VARCHAR_LOB,
    MD5((generate_series * random())::text) as C_NATIONAL_CHARACTER,
    MD5((generate_series * random())::text) as C_NATIONAL_CHARACTER_VAR,
    now() as C_DATE,
    current_time as C_TIME_WITHOUT_TIMEZONE,
    current_timestamp as C_TIMESTAMP_WITHOUT_TIMEZONE,
    current_time at time zone 'cet' as C_TIME_WITH_TIMEZONE,
    current_timestamp at time zone 'cet' as C_TIMESTAMP_WITH_TIMEZONE,
    ARRAY [(generate_series * random())::text,(generate_series * random())::text],
    ARRAY [[(generate_series * random())::int,(generate_series * random())::int],[(generate_series * random())::int,(generate_series * random())::int]],
    ('<person><age>'||(generate_series * random())::integer||'</age><firstName>'||(generate_series * random())::text||'</firstName><lastName>'||(generate_series * random())::text||'</lastName></person>')::xml,
    ('{"firstName": "'||(generate_series * random())::integer||'",  "lastName": "'||(generate_series * random())::integer||'",  "age": '||(generate_series * random())::integer||'}')::jsonb
from generate_series_4k;

-- Null values
insert into t_source (
    C_SMALLINT                   ,
    C_BIGINT                     ,
    C_NUMERIC                    ,
    C_DECIMAL                    ,
    C_REAL                       ,
    C_DOUBLE_PRECISION           ,
    C_FLOAT                      ,
    C_BINARY                     ,
    C_BINARY_VAR                 ,
    C_BINARY_LOB                 ,
    C_BOOLEAN                    ,
    C_CHARACTER                  ,
    C_CHARACTER_VAR              ,
    C_CHARACTER_LOB              ,
    C_NATIONAL_CHARACTER         ,
    C_NATIONAL_CHARACTER_VAR     ,
    C_DATE                       ,
    C_TIME_WITHOUT_TIMEZONE      ,
    C_TIMESTAMP_WITHOUT_TIMEZONE ,
    C_TIME_WITH_TIMEZONE         ,
    C_TIMESTAMP_WITH_TIMEZONE    ,
    C_INTERVAL_DAY               ,
    C_INTERVAL_YEAR              ,
    C_ARRAY                      ,
    C_MULTIDIMENSIONAL_ARRAY     ,
    C_MULTISET                   ,
    C_XML                        ,
    C_JSON
)
values (null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null );