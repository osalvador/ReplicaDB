create table if not exists public.t_source (
    c_smallserial smallserial,
    c_serial serial,
    c_bigserial bigserial,
    c_smallint smallint,
    c_integer integer,
    c_bigint bigint,
    c_decimal decimal,
    c_numeric numeric,
    c_real real,
    c_double double precision,
    c_money money,
    c_varchar varchar(10),
    c_char char(2),
    c_text text,
    c_bytea bytea,
    c_date date,
    c_timestamp timestamp,
    c_timestamptz timestamp with time zone,
    c_time time,
    c_timetz time with time zone,
    c_boolean boolean,
    c_point point,
    c_line line,
    c_lseg lseg,
    c_box box,
    c_path path,
    c_polygon polygon,
    c_circle circle,
    c_xml xml,
    c_json json,
    c_jsonb jsonb,
    c_array integer[],
    c_array2 text[][],
    PRIMARY KEY (c_serial)
);

insert into  public.t_source (
    --c_smallserial,
    --c_serial,
    --c_bigserial,
    c_smallint,
    c_integer,
    c_bigint,
    c_decimal,
    c_numeric,
    c_real,
    c_double,
    c_money,
    c_varchar,
    c_char,
    c_text,
    c_bytea,
    c_date,
    c_timestamp,
    c_timestamptz,
    c_time,
    c_timetz,
    c_boolean,
    c_point,
    c_line,
    c_lseg,
    c_box,
    c_path,
    c_polygon,
    c_circle,
    c_xml,
    c_json,
    c_jsonb,
    c_array,
    c_array2
)
WITH numbers AS (
  SELECT *
  FROM generate_series(1, 10000)
)
SELECT
    (generate_series * random())::smallint,
    (generate_series * random())::integer,
    (generate_series * random())::bigint,
    (generate_series * random())::decimal,
    (generate_series * random())::numeric,
    (generate_series * random())::real,
    (generate_series * random())::double precision,
    (generate_series * random())::numeric::money,
    left(MD5((generate_series * random())::text),10),
    left(MD5((generate_series * random())::text),2),
    MD5((generate_series * random())::text),
    '\xDEADBEEF'::bytea,
    now(),
    current_timestamp,
    current_timestamp,
    current_time,
    current_time,
    true,
    '-81.4,30.3'::point,
    '(-81.4,30.3),(-81.3,30.4)'::line,
    '(-81.4,30.3),(-81.3,30.4)'::lseg,
    '(-81.4,30.3),(-81.3,30.4)'::box,
    '(-81.4,30.3),(-81.3,30.4)'::path,
    '(-81.4,30.3),(-81.3,30.4)'::polygon,
    '(-81.4,30.3),44.02'::circle,
    '<person><name>Jon</name><age>22</age></person>'::xml,
    '{"bar": "baz", "balance": 7.77, "active":false}'::json,
    '{"bar": "baz", "balance": 7.77, "active":false}'::jsonb,
    '{10000, 10000, 10000, 10000}'::integer[],
    '{{"meeting", "lunch"}, {"training", "presentation"}}'::text[][]
FROM numbers;


/*

CREATE TABLE Towns (
  id SERIAL UNIQUE NOT NULL,
  code VARCHAR(10) NOT NULL, -- not unique
  article TEXT,
  name TEXT NOT NULL, -- not unique
  department VARCHAR(4) NOT NULL REFERENCES Departments (code),
  UNIQUE (code, department)
);

insert into towns (
    code, article, name, department
)
select
    left(md5(i::text), 10),
    md5(random()::text),
    md5(random()::text),
    left(md5(random()::text), 4)
from generate_series(1, 1000000) s(i)


*/

