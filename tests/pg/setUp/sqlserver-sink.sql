create table dbo.t_sink (
    c_identity integer identity ,
    c_smallint smallint,
    c_integer integer,
    c_bigint bigint,
    c_decimal decimal(30,15),
    c_numeric numeric(30,15),
    c_real real,
    c_double double	precision,
    c_money money,
    c_varchar nvarchar(10),
    c_char char(2),
    c_text nvarchar(max),
    c_bytea VARBINARY(MAX),
    c_date date,
    c_datetime datetime,
    c_datetime_no_tz datetime,
    c_time time,
    c_time_no_tz time,
    c_boolean bit,
    c_point nvarchar(max),
    c_line nvarchar(max),
    c_lseg nvarchar(max),
    c_box nvarchar(max),
    c_path nvarchar(max),
    c_polygon nvarchar(max),
    c_circle nvarchar(max),
    c_xml xml,
    c_json nvarchar(max),
    c_jsonb nvarchar(max),
    c_array nvarchar(max),
    c_array2 nvarchar(max),
    "FOR" nvarchar(max),
    PRIMARY KEY (c_identity)
);

ALTER TABLE dbo.t_sink
    ADD CONSTRAINT [c_json record should be formatted as JSON]
                   CHECK (ISJSON(c_json)=1);
ALTER TABLE dbo.t_sink
    ADD CONSTRAINT [c_jsonb record should be formatted as JSON]
                   CHECK (ISJSON(c_jsonb)=1);