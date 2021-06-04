create table t_sink (
  C_INT number (11,0),
  C_TINYINT number(2,0),
  C_SMALLINT number(5,0),
  C_MEDIUMINT number(7,0),
  C_BIGINT number(20,0),
  C_DECIMAL DECIMAL(30,15),
  C_FLOAT DECIMAL(30,15),
  C_BIT char(1),
  C_CHAR char(1),
  C_VARCHAR VARCHAR2(2000),
  C_BINARY BLOB,
  C_VARBINARY BLOB,
  C_TINYBLOB BLOB,
  C_BLOB BLOB,
  C_MEDIUMBLOB BLOB,
  C_LONGBLOB BLOB,
  C_TINYTEXT VARCHAR2(100),
  C_TEXT VARCHAR2(1000),
  C_MEDIUMTEXT VARCHAR2(1000),
  C_LONGTEXT CLOB,
  PRIMARY KEY (C_INT)
);