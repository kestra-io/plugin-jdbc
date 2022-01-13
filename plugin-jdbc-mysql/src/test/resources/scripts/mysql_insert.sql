
USE kestra;

DROP TABLE IF EXISTS mysql_types;
DROP TABLE IF EXISTS namedInsert;

CREATE TABLE mysql_types (
 available TINYINT not null,
 a CHAR(4) not null,
 b VARCHAR(30) not null,
 c TEXT not null,
 play_time BIGINT not null,
 library_record BIGINT not null,
 bitn_test BIT(6) not null,
 double_test DOUBLE not null,
 doublen_test DOUBLE(18,4) not null,
 numeric_test NUMERIC(3,2) not null,
 salary_decimal DECIMAL(5,2),
 date_type DATE not null,
 datetime_type DATETIME(6) not null,
 time_type TIME not null,
 timestamp_type TIMESTAMP(6) not null,
 year_type YEAR(4) not null,
 json_type JSON not null,
 blob_type BLOB not null
);

CREATE TABLE namedInsert (
 id integer,
 name VARCHAR(255),
 address VARCHAR(255)
 );