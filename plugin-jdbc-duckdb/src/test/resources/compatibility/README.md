# Cross-version type audit

`types.txt` covers 44 expressions, including signed/unsigned numeric boundaries,
DECIMAL precision, pre/post-epoch timestamps, nanoseconds, new timezone
nanoseconds, escaped/nested containers, all major scalar/container types, and
both typed and untyped NULL. Each expression is queried with a second NULL row.
The recorded audit first attempts one wide query, then runs each column
separately so an unsupported type cannot hide the rest of the matrix.

The JSON reports are actual runs through `AbstractJdbcQuery` with the original
converter at `22f0b165` + JDBC 1.5.6.0, and the updated converter + JDBC 2.0.0.1.
They record raw JDBC metadata/classes and results from FETCH, FETCH_ONE and
STORE (the latter decoded from Kestra's Ion file using FileSerde). JVM and
serialization timezones are UTC. These are value/wire-format comparisons, not
byte-for-byte comparisons of timestamp formatting in arbitrary JVM timezones.

`DuckDbTypeMatrixTest` checks the recorded baseline against the expected 2.0
values and re-runs every individual expression through the actual DuckDB Query
task. Two deliberate differences:

- VARIANT changes from an opaque struct string to a map/list/scalar value.
- TIMESTAMPTZ_NS does not exist in 1.5.6; 2.0 returns a string from JDBC, which
  the converter now parses to a UTC OffsetDateTime without losing nanoseconds.

JDBC metadata also deliberately changes an untyped NULL from INTEGER to
`"NULL"`, including the corresponding field in STRUCT. Values remain null, so
these two metadata changes require no converter rewrite. All other existing
JDBC type/class names in the matrix are unchanged.

TIMETZ, BLOB, MAP and GEOMETRY already failed in 1.5.6. The tests require those
same explicit unsupported-type errors on 2.0. This is a documented limitation,
not newly added support. TIME_NS and ordinary TIMESTAMP_NS preserve the previous
wire values; the existing converter rounds ordinary timestamp nanoseconds to
its established precision. No claim of nanosecond support for those old paths
is made. The wide query's failure is retained in each report rather than hidden.

## Run the regression tests

From the repository root, with Java 25:

```sh
./gradlew :plugin-jdbc-duckdb:test --tests '*DuckDbTypeMatrixTest'
```

Keep the recorded JSON files fixed when changing the production driver. They
are test inputs, including the original 1.5.6 output, not reports regenerated
by the test itself. Inspect and explain any deliberate expectation changes.
