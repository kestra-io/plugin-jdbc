CREATE DATABASE IF NOT EXISTS kestra;

DROP TABLE IF EXISTS clickhouse_types;

CREATE TABLE clickhouse_types (
   Int8 Int8,
   Float32 Float32,
   Float64 Float64,
   Decimal Decimal(14, 4),
   String String,
   FixedString FixedString(4),
   Uuid UUID,
   Date Date,
   DateTime DateTime('Europe/Moscow'),
   DateTimeNoTZ DateTime,
   DateTime64 DateTime64(3, 'Europe/Moscow'),
   Enum Enum('hello' = 1, 'world' = 2),
   LowCardinality LowCardinality(String),
   Array Array(String),
--    AggregateFunction AggregateFunction(uniq, UInt64),
   Nested Nested(
       NestedId Int8,
       NestedString String
   ),
   Tuple Tuple(String, Int8),
   Nullable Nullable(Int8),
   Ipv4 IPv4,
   Ipv6 IPv6
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(DateTime)
ORDER BY (Int8)
SETTINGS index_granularity = 8192;


-- Insert
INSERT INTO clickhouse_types
(
    Int8,
    Float32,
    Float64,
    Decimal,
    String,
    FixedString,
    Uuid,
    Date,
    DateTime,
    DateTimeNoTZ,
    DateTime64,
    Enum,
    LowCardinality,
    Array,
--     AggregateFunction,
    Nested.NestedId,
    Nested.NestedString,
    Tuple,
    Nullable,
    Ipv4,
    Ipv6
)
VALUES (
    123,
    2147483645.1234,
    2147483645.1234,
    2147483645.1234,
    'four',
    'four',
    '6bbf0744-74b4-46b9-bb05-53905d4538e7',
    '2030-12-25',
    '2004-10-19T10:23:54',
    '2004-10-19T10:23:54',
    '2004-10-19T10:23:54.999999',
    'hello',
    'four',
    ['a', 'b'],
--     uniqState(123),
    [123],
    ['four'],
    tuple('a', 1) ,
    NULL,
    '116.253.40.133',
    '2a02:aa08:e000:3100::2'
);


select * from clickhouse_types;
