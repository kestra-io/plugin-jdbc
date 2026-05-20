# How to use the JDBC plugin

Run SQL queries, execute batch inserts, and trigger flows from query results across 17+ databases including MySQL, PostgreSQL, Snowflake, DuckDB, and more.

## Common properties

Set `url`, `username`, and `password` directly on each task. Use [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults) to avoid repeating connection details across tasks in the same flow. Store credentials in [secrets](https://kestra.io/docs/concepts/secret).

## Tasks

`Query` executes a single SQL statement. Control output with `fetchType`: `FETCH_ONE` returns the first row as a map, `FETCH` returns all rows as a list, `STORE` streams rows to a file in internal storage (best for large result sets), and `NONE` discards results. Use `parameters` for named bindings (`:paramName` syntax) and `afterSQL` to run a follow-up statement in the same transaction — useful for marking rows as processed after reading them.

`Queries` executes multiple semicolon-separated statements in a single operation. With `transaction: true` (the default), a failure rolls back all statements.

`Batch` bulk-inserts rows from a Kestra internal storage file into a table. Set `from` to the source file URI and either `sql` (a parameterized INSERT with `?` placeholders) or `table` to auto-generate the statement. Tune throughput with `chunk` (rows per batch commit, default 1000).

`Trigger` polls a database on a schedule and starts one execution per batch of matching rows — use it to react to new records in a queue or staging table.

Select the task class for your database — for example `io.kestra.plugin.jdbc.mysql.Query` for MySQL or `io.kestra.plugin.jdbc.postgres.Query` for PostgreSQL. Each database-specific submodule exposes the same task interface with additional driver-level options where applicable.
