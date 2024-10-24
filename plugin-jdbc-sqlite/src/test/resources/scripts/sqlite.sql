DROP TABLE IF EXISTS lite_types;

-- Create a table with columns resembling the requested data types
CREATE TABLE lite_types (
    id INTEGER PRIMARY KEY,
    boolean_column BOOLEAN,
    text_column TEXT,
    float_column REAL, -- SQLite doesn't have a FLOAT data type, REAL is used for floating-point values
    double_column REAL, -- Use REAL for double-precision floating-point values
    int_column INTEGER,
    date_column DATE,
    datetime_column DATETIME,
    time_column TIME,
    timestamp_column TIMESTAMP,
    year_column INTEGER, -- SQLite doesn't have a YEAR data type, use INTEGER
    json_column TEXT, -- SQLite doesn't have a native JSON data type
    blob_column BLOB,
    d TEXT
);

-- Insert sample data into the table
INSERT INTO lite_types (
    boolean_column,
    text_column,
    float_column,
    double_column,
    int_column,
    date_column,
    datetime_column,
    time_column,
    timestamp_column,
    year_column,
    json_column,
    blob_column,
    d
) VALUES (
    1,
    'Sample Text',
    3.14,
    3.14159265359,
    42,
    '2023-10-30',
    '2023-10-30 22:59:57.150150',
    '14:30:00',
    '2023-10-30 14:30:00.000',
    2023,
    '{"key": "value"}',
    X'0102030405060708',
    NULL
);