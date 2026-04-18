# Kestra JDBC Plugin

## What

- Provides plugin components under `io.kestra.plugin`.
- Keeps the implementation focused on the integration scope exposed by this repository.

## Why

- This plugin integrates Kestra with JDBC.
- It adds workflow components that reflect the code in this repository.

## How

### Architecture

This is a **multi-module** plugin with 22 submodules:

- `plugin-jdbc`
- `plugin-jdbc-actianvector`
- `plugin-jdbc-arrow-flight`
- `plugin-jdbc-as400`
- `plugin-jdbc-clickhouse`
- `plugin-jdbc-db2`
- `plugin-jdbc-dremio`
- `plugin-jdbc-druid`
- `plugin-jdbc-duckdb`
- `plugin-jdbc-hana`
- `plugin-jdbc-mariadb`
- `plugin-jdbc-mysql`
- `plugin-jdbc-oracle`
- `plugin-jdbc-pinot`
- `plugin-jdbc-postgres`
- `plugin-jdbc-redshift`
- `plugin-jdbc-snowflake`
- `plugin-jdbc-sqlite`
- `plugin-jdbc-sqlserver`
- `plugin-jdbc-sybase`
- `plugin-jdbc-trino`
- `plugin-jdbc-vertica`

Infrastructure dependencies (Docker Compose services):

- `broker_var {}`
- `clickhouse`
- `coordinator_var {}`
- `dremio`
- `druid_broker`
- `druid_coordinator`
- `druid_historical`
- `druid_middlemanager`
- `druid_postgres`
- `druid_router`
- `druid_shared {}`
- `druid_zookeeper`
- `historical_var {}`
- `mariadb`
- `metadata_data {}`
- `middle_var {}`
- `mysql`
- `oracle`
- `pinot`
- `postgres`
- `postgres-multi-query`
- `router_var {}`
- `sqlserver`
- `trino`

### Key Plugin Classes

**plugin-jdbc-actianvector:**

- `io.kestra.plugin.jdbc.actianvector.Batch`
- `io.kestra.plugin.jdbc.actianvector.Queries`
- `io.kestra.plugin.jdbc.actianvector.Query`
- `io.kestra.plugin.jdbc.actianvector.Trigger`
**plugin-jdbc-arrow-flight:**

- `io.kestra.plugin.jdbc.arrowflight.Queries`
- `io.kestra.plugin.jdbc.arrowflight.Query`
- `io.kestra.plugin.jdbc.arrowflight.Trigger`
**plugin-jdbc-as400:**

- `io.kestra.plugin.jdbc.as400.Queries`
- `io.kestra.plugin.jdbc.as400.Query`
- `io.kestra.plugin.jdbc.as400.Trigger`
**plugin-jdbc-clickhouse:**

- `io.kestra.plugin.jdbc.clickhouse.BulkInsert`
- `io.kestra.plugin.jdbc.clickhouse.ClickHouseLocalCLI`
- `io.kestra.plugin.jdbc.clickhouse.Queries`
- `io.kestra.plugin.jdbc.clickhouse.Query`
- `io.kestra.plugin.jdbc.clickhouse.Trigger`
**plugin-jdbc-db2:**

- `io.kestra.plugin.jdbc.db2.Queries`
- `io.kestra.plugin.jdbc.db2.Query`
- `io.kestra.plugin.jdbc.db2.Trigger`
**plugin-jdbc-dremio:**

- `io.kestra.plugin.jdbc.dremio.Queries`
- `io.kestra.plugin.jdbc.dremio.Query`
- `io.kestra.plugin.jdbc.dremio.Trigger`
**plugin-jdbc-druid:**

- `io.kestra.plugin.jdbc.druid.Queries`
- `io.kestra.plugin.jdbc.druid.Query`
- `io.kestra.plugin.jdbc.druid.Trigger`
**plugin-jdbc-duckdb:**

- `io.kestra.plugin.jdbc.duckdb.Queries`
- `io.kestra.plugin.jdbc.duckdb.Query`
- `io.kestra.plugin.jdbc.duckdb.Trigger`
**plugin-jdbc-hana:**

- `io.kestra.plugin.jdbc.hana.Queries`
- `io.kestra.plugin.jdbc.hana.Query`
- `io.kestra.plugin.jdbc.hana.Trigger`
**plugin-jdbc-mariadb:**

- `io.kestra.plugin.jdbc.mariadb.Queries`
- `io.kestra.plugin.jdbc.mariadb.Query`
- `io.kestra.plugin.jdbc.mariadb.Trigger`
**plugin-jdbc-mysql:**

- `io.kestra.plugin.jdbc.mysql.Batch`
- `io.kestra.plugin.jdbc.mysql.Queries`
- `io.kestra.plugin.jdbc.mysql.Query`
- `io.kestra.plugin.jdbc.mysql.Trigger`
**plugin-jdbc-oracle:**

- `io.kestra.plugin.jdbc.oracle.Batch`
- `io.kestra.plugin.jdbc.oracle.Queries`
- `io.kestra.plugin.jdbc.oracle.Query`
- `io.kestra.plugin.jdbc.oracle.Trigger`
**plugin-jdbc-pinot:**

- `io.kestra.plugin.jdbc.pinot.Queries`
- `io.kestra.plugin.jdbc.pinot.Query`
- `io.kestra.plugin.jdbc.pinot.Trigger`
**plugin-jdbc-postgres:**

- `io.kestra.plugin.jdbc.postgresql.Batch`
- `io.kestra.plugin.jdbc.postgresql.CopyIn`
- `io.kestra.plugin.jdbc.postgresql.CopyOut`
- `io.kestra.plugin.jdbc.postgresql.Queries`
- `io.kestra.plugin.jdbc.postgresql.Query`
- `io.kestra.plugin.jdbc.postgresql.Trigger`
**plugin-jdbc-redshift:**

- `io.kestra.plugin.jdbc.redshift.Queries`
- `io.kestra.plugin.jdbc.redshift.Query`
- `io.kestra.plugin.jdbc.redshift.Trigger`
**plugin-jdbc-snowflake:**

- `io.kestra.plugin.jdbc.snowflake.Download`
- `io.kestra.plugin.jdbc.snowflake.Queries`
- `io.kestra.plugin.jdbc.snowflake.Query`
- `io.kestra.plugin.jdbc.snowflake.SnowflakeCLI`
- `io.kestra.plugin.jdbc.snowflake.Trigger`
- `io.kestra.plugin.jdbc.snowflake.Upload`
**plugin-jdbc-sqlite:**

- `io.kestra.plugin.jdbc.sqlite.Queries`
- `io.kestra.plugin.jdbc.sqlite.Query`
- `io.kestra.plugin.jdbc.sqlite.Trigger`
**plugin-jdbc-sqlserver:**

- `io.kestra.plugin.jdbc.sqlserver.Batch`
- `io.kestra.plugin.jdbc.sqlserver.Queries`
- `io.kestra.plugin.jdbc.sqlserver.Query`
- `io.kestra.plugin.jdbc.sqlserver.Trigger`
**plugin-jdbc-sybase:**

- `io.kestra.plugin.jdbc.sybase.Queries`
- `io.kestra.plugin.jdbc.sybase.Query`
- `io.kestra.plugin.jdbc.sybase.Trigger`
**plugin-jdbc-trino:**

- `io.kestra.plugin.jdbc.trino.Query`
- `io.kestra.plugin.jdbc.trino.Trigger`
**plugin-jdbc-vertica:**

- `io.kestra.plugin.jdbc.vertica.Batch`
- `io.kestra.plugin.jdbc.vertica.Queries`
- `io.kestra.plugin.jdbc.vertica.Query`
- `io.kestra.plugin.jdbc.vertica.Trigger`

### Project Structure

```
plugin-jdbc/
├── plugin-jdbc/
│   └── src/main/java/...
├── ...                                    # Other submodules
├── build.gradle
├── settings.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
