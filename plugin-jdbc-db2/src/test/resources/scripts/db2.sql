-- Drop the db2_types table if it exists
DROP TABLE IF EXISTS DB2INST1.db2_types;

-- Create the db2_types table
CREATE TABLE DB2INST1.db2_types (
    ID INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    INTEGER_col INT,
    BIGINT_col BIGINT,
    DECIMAL_col DECIMAL(10,2),
    REAL_col REAL,
    DOUBLE_col DOUBLE,
    CHARACTER_col CHAR,
    VARCHAR_col VARCHAR(255),
    GRAPHIC_col GRAPHIC,
    VARGRAPHIC_col VARGRAPHIC(255),
    DATE_col DATE,
    TIME_col TIME,
    TIMESTAMP_col TIMESTAMP,
    BLOB_col BLOB(1024),
    CLOB_col CLOB(1024),
    XML_col XML
);

-- Insert values into the db2_types table
INSERT INTO DB2INST1.db2_types
    (INTEGER_col, BIGINT_col, DECIMAL_col, REAL_col, DOUBLE_col, CHARACTER_col, VARCHAR_col, GRAPHIC_col, VARGRAPHIC_col, DATE_col, TIME_col, TIMESTAMP_col)
VALUES
    (123, 1234567890123456789, 123.45, 123.45, 123.45, 'c', 'var character', 'g', 'var graphic', '2024-03-22', '12:51:25', '2024-03-22T12:51:25');