CREATE TABLE sqlserver_types (
    t_bigint BIGINT,
    t_int INT,
    t_smallint SMALLINT,
    t_tinyint TINYINT,
    t_float FLOAT,
    t_real REAL,
    t_decimal DECIMAL(5, 2),
    t_numeric NUMERIC(10, 5),
    t_bit BIT,
    t_smallmoney SMALLMONEY,
    t_money MONEY,
    t_char CHAR(10),
    t_varchar VARCHAR(10),
    t_nchar NCHAR(10),
    t_nvarchar NVARCHAR(10),
    t_text TEXT,
    t_ntext NTEXT,
    t_time TIME,
    t_date DATE,
    t_smalldatetime SMALLDATETIME,
    t_datetime DATETIME,
    t_datetime2 DATETIME2,
    t_datetimeoffset DATETIMEOFFSET,
);

CREATE TABLE namedInsert (
    t_id INT,
    t_name VARCHAR(20),
    t_address VARCHAR(20)
);

