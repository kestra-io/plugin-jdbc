CREATE DATABASE IF NOT EXISTS kestra;

DROP TABLE IF EXISTS clickhouse_types;
DROP TABLE IF EXISTS namedInsertNested;
DROP TABLE IF EXISTS namedInsert;

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
   DateTime64 DateTime64(3, 'Europe/Moscow'),
   Enum Enum('hello' = 1, 'world' = 2),
   LowCardinality LowCardinality(String),
   Array Array(String),
--   Nested Nested(
--       NestedId Int8,
--       NestedString String
--   ),
--   Tuple Tuple(String, Int8),
   Ipv4 IPv4,
   Ipv6 IPv6
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(DateTime)
ORDER BY (Int8)
SETTINGS index_granularity = 8192;

CREATE TABLE namedInsertNested (
    id Int8,
    nested Nested(
        age Int8,
        address String
    )
)
ENGINE = MergeTree()
ORDER BY (id)
SETTINGS index_granularity = 8192;

CREATE TABLE namedInsert (
    id Int8,
    name String,
    address String
)
ENGINE = MergeTree()
ORDER BY (id)
SETTINGS index_granularity = 8192;