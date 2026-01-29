create table t_sink
(
/*exact numerics*/
    c_integer                    integer,
    c_smallint                   smallint,
    c_bigint                     bigint,
    c_numeric                   numeric(30,15),
    c_decimal                    decimal(30, 15),
    /*approximate numerics:*/
    c_real                       real,
    c_double_precision           float,
    c_float                      float,
    /*binary strings:*/
    c_binary                     varbinary(35),
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
    /*other types:*/
    c_xml                        xml,
    primary key (c_integer)
);
