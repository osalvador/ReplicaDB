CREATE VIEW generate_series_16
AS SELECT 0 generate_series UNION ALL SELECT 1  UNION ALL SELECT 2  UNION ALL
   SELECT 3   UNION ALL SELECT 4  UNION ALL SELECT 5  UNION ALL
   SELECT 6   UNION ALL SELECT 7  UNION ALL SELECT 8  UNION ALL
   SELECT 9   UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL
   SELECT 12  UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL
   SELECT 15;

CREATE VIEW generate_series_256
AS SELECT ( hi.generate_series * 16 + lo.generate_series ) AS generate_series
   FROM generate_series_16 lo, generate_series_16 hi;

CREATE VIEW generate_series_4k
AS SELECT ( hi.generate_series * 256 + lo.generate_series ) AS generate_series
   FROM generate_series_256 lo, generate_series_16 hi;

create table t_source
(
/*exact numerics*/
    c_integer                    integer not null identity (1,1),
    c_smallint                   smallint,
    c_bigint                     bigint,
    c_numeric                    numeric(30, 15),
    c_decimal                    decimal(30, 15),
    /*approximate numerics:*/
    c_real                       decimal(30, 15),
    c_double_precision           decimal(30, 15),
    c_float                      decimal(30, 15),
    /*binary strings:*/
    c_binary                     varbinary(35),
    c_binary_var                 varbinary(255),
    c_binary_lob                 varbinary(max),
    /*boolean:*/
    c_boolean                    bit,
    /*character strings:*/
    c_character                  char(35),
    c_character_var              varchar(255),
    c_character_lob              varchar(max),
    c_national_character         nchar(35),
    c_national_character_var     nvarchar(255),
    /*datetimes:*/
    c_date                       date,
    c_time_without_timezone      time,
    c_timestamp_without_timezone datetime,
    /*other types:*/
    c_xml                        xml,
    primary key (c_integer)
);

insert into t_source (
    /*c_integer, auto incremented*/
    c_smallint,
    c_bigint,
    c_numeric,
    c_decimal,
    c_real,
    c_double_precision,
    c_float,
    c_binary,
    c_binary_var,
    c_binary_lob,
    c_boolean,
    c_character,
    c_character_var,
    c_character_lob,
    c_national_character,
    c_national_character_var,
    c_date,
    c_time_without_timezone,
    c_timestamp_without_timezone,
    c_xml)
select checksum(newid()) % 100                          as c_smallint,
       checksum(newid()) % 1000                         as c_bigint,
       checksum(newid()) * rand()                       as c_numeric,
       checksum(newid()) * rand()                       as c_decimal,
       checksum(newid()) * rand()                       as c_real,
       checksum(newid()) * rand()                       as c_double_precision,
       checksum(newid()) * rand()                       as c_float,
       convert(binary(16), newid())                     as c_binary,
       convert(binary(255), newid())                    as c_binary_var,
       convert(binary(1000), newid())                   as c_binary_lob,
       1,
       substring(convert(varchar(40), newid()), 0, 35)  as c_character,
       convert(varchar(255), newid())                   as c_character_var,
       convert(varchar(1000), newid())                  as c_character_lob,
       substring(convert(nvarchar(40), newid()), 0, 35) as c_national_character,
       convert(nvarchar(255), newid())                  as c_national_character_var,
       getdate()                                        as c_date,
       CONVERT(TIME, GETDATE())                         as c_time_without_timezone,
       current_timestamp                                as c_timestamp_without_timezone,
       convert(xml, '<xml>replicadb</xml>')             as c_xml
from generate_series_4k;

insert into t_source (
    c_smallint                   ,
    c_bigint                     ,
    c_numeric                    ,
    c_decimal                    ,
    c_real                       ,
    c_double_precision           ,
    c_float                      ,
    c_binary                     ,
    c_binary_var                 ,
    c_binary_lob                 ,
    c_boolean                    ,
    c_character                  ,
    c_character_var              ,
    c_character_lob              ,
    c_national_character         ,
    c_national_character_var     ,
    c_date                       ,
    c_time_without_timezone      ,
    c_timestamp_without_timezone ,
    c_xml
)
values (null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null);