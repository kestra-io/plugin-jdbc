
DROP TABLE IF EXISTS vertica_types;
DROP TABLE IF EXISTS namedInsert;
CREATE TABLE vertica_types (
   "binary" BINARY,
   varbinary VARBINARY,
   long_varbinary LONG VARBINARY,
   bytea BYTEA,
   raw RAW,
   "boolean" BOOLEAN,
   char CHAR(4),
   varchar VARCHAR,
   long_varchar LONG VARCHAR,
   date DATE,
   time TIME,
   datetime DATETIME,
   smalldatetime SMALLDATETIME,
   time_with_timezone TIME WITH TIMEZONE,
   timestamp TIMESTAMP,
   timestamp_with_timezone TIMESTAMP WITH TIMEZONE,
   "interval" INTERVAL,
   interval_day_to_second INTERVAL DAY TO SECOND,
   interval_year_to_month INTERVAL YEAR TO MONTH,
   double_precision DOUBLE PRECISION,
   float FLOAT,
   float_n FLOAT(8),
   float8 FLOAT8,
   real REAL,
   integer INTEGER,
   int INT,
   bigint BIGINT,
   int8 INT8,
   smallint SMALLINT,
   tinyint TINYINT,
   decimal DECIMAL(14, 4),
   numeric NUMERIC(14, 4),
   number NUMBER(14, 4),
   money MONEY(14, 4)
--   geometry GEOMETRY,
--   geography GEOGRAPHY,
--   uuid UUID
);

CREATE TABLE namedInsert (
 id integer,
 name VARCHAR,
 address VARCHAR
);
