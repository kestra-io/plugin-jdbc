IF EXISTS(select 1 from sysobjects where name='syb_types' and type='U')
  DROP TABLE syb_types;

CREATE TABLE syb_types(
    concert_id INT IDENTITY PRIMARY KEY,
    available TINYINT NOT NULL,
    a CHAR(4) NOT NULL,
    b VARCHAR(30) NOT NULL,
    c TEXT NOT NULL,
    d VARCHAR(10) NULL,
    play_time BIGINT NOT NULL,
    library_record BIGINT NOT NULL,
    bitn_test BIT NOT NULL,
    floatn_test REAL,
    double_test FLOAT,
    doublen_test FLOAT,
    numeric_test DECIMAL(5,2),
    salary_decimal DECIMAL(5,2),
    date_type DATE NOT NULL,
    datetime_type DATETIME NOT NULL,
    time_type TIME NOT NULL,
    timestamp_type DATETIME NOT NULL,
    blob_type IMAGE NOT NULL -- BLOB type is not directly supported in Sybase ASE, using IMAGE as an alternative
);

-- Insert
INSERT INTO syb_types
            (available,
             a,
             b,
             c,
			 d,
             play_time,
             library_record,
             bitn_test,
             floatn_test,
             double_test,
             doublen_test,
             numeric_test,
             salary_decimal,
             date_type,
             datetime_type,
             time_type,
             timestamp_type,
             blob_type)
VALUES     (
1,
 'four',
  'This is a varchar',
    'This is a text column data',
     NULL,
      -9223372036854775808,
       1844674407370955161,
        1,
         9.223372,
          9.223372,
           2147483645.1234,
            5.36,
             999.99,
              '2030-12-25',
               '2050-12-31 22:59:57.150150',
                '04:05:30',
                 '2004-10-19 10:23:54.999999',
                    0x123456
                    );