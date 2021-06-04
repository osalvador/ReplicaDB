
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
    C_INT INT  AUTO_INCREMENT,
    C_TINYINT TINYINT,
    C_SMALLINT SMALLINT,
    C_MEDIUMINT MEDIUMINT,
    C_BIGINT BIGINT,
    C_DECIMAL DECIMAL(65,30),
    C_FLOAT FLOAT,
    C_BIT BIT,
    C_CHAR CHAR,
    C_VARCHAR VARCHAR(2000),
    C_BINARY BINARY(255),
    C_VARBINARY VARBINARY(255),
    C_TINYBLOB TINYBLOB,
    C_BLOB BLOB,
    C_MEDIUMBLOB MEDIUMBLOB,
    C_LONGBLOB LONGBLOB,
    C_TINYTEXT TINYTEXT,
    C_TEXT TEXT,
    C_MEDIUMTEXT MEDIUMTEXT,
    C_LONGTEXT LONGTEXT,
    PRIMARY KEY (C_INT)
);

insert into t_source (
    C_TINYINT,
    C_SMALLINT,
    C_MEDIUMINT,
    C_BIGINT,
    C_DECIMAL,
    C_FLOAT,
    C_BIT,
    C_CHAR,
    C_VARCHAR,
    C_BINARY
)
select
    substr(CAST(generate_series * rand() as signed),1,2) as C_TINYINT,
    CAST(generate_series * rand() as signed) as C_SMALLINT,
    CAST(generate_series * rand() as signed) as C_MEDIUMINT,
    CAST(generate_series * rand() as signed) as C_BIGINT,
    CAST(generate_series * rand() as decimal(65, 30)) as C_DECIMAL,
    CAST(generate_series * rand() as decimal(25, 10)) as C_FLOAT,
    1 as C_BIT,
    substr(md5(rand()),1,1) as C_CHAR,
    md5(rand()) as C_VARCHAR,
    BINARY(md5(rand())) as C_BINARY
from generate_series_4k;
