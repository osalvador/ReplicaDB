create table t_sink
(
/*exact numerics*/
    c_integer                    integer,
    c_tinyint                    tinyint,
    c_smallint                   smallint,
    c_bigint                     bigint,
    c_numeric                   numeric(30,15),
    c_decimal                    decimal(30, 15),
    /*approximate numerics:*/
    c_real                       decimal(30, 15),
    c_double_precision           decimal(30, 15),
    c_float                      decimal(30, 15),
    /*binary strings:*/
    c_binary                     image,
    c_binary_var                 varbinary(max),
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
    c_time_with_timezone         varchar(100) /*not supported*/,
    c_timestamp_with_timezone    datetime,
    /*intervals:*/
    c_interval_day               varchar(100)  /*not supported*/,
    c_interval_year              varchar(100)  /*not supported*/,
    /*collection types:*/
    c_array                        varchar(max),/*not supported*/
    c_multidimensional_array       varchar(max)/*not supported*/,
    c_multiset                     varchar(max), /*not supported*/
    /*other types:*/
    --         row /*not supported*/
    c_xml                        xml,
    c_json                       nvarchar(max) /*not supported*/,
    primary key (c_integer)
);