# Progress OpenEdge JDBC

The OpenEdge module provides three read-side JDBC tasks:

- `io.kestra.plugin.jdbc.openedge.Query` executes one SQL statement.
- `io.kestra.plugin.jdbc.openedge.Queries` executes multiple SQL statements.
- `io.kestra.plugin.jdbc.openedge.Trigger` polls a query and starts a flow when rows are returned.

There is intentionally no `Batch` task: the requested use case is extracting data from OpenEdge, with PostgreSQL already available as a destination through `plugin-jdbc-postgresql`.

## Driver requirement and installation

The Progress DataDirect OpenEdge JDBC driver is proprietary and is **not bundled** with this plugin or published as a dependency from this project. Obtain the driver from Progress under the applicable OpenEdge license and maintenance terms.

The driver class is `com.ddtek.jdbc.openedge.OpenEdgeDriver`. The module discovers this fixed driver through the standard JDBC service-provider mechanism using the OpenEdge plugin classloader, then registers the discovered driver with `DriverManager`. It does not use reflective class loading and does not accept a configurable driver class name or filesystem path. Progress documents automatic driver registration for Java SE 6 and later; the supplied driver must expose its standard JDBC service-provider entry.

For self-hosted Kestra, place the licensed driver JARs in the same plugin directory as this module so that they are loaded by the same plugin classloader. For example:

```text
plugins/
└── openedge/
    ├── plugin-jdbc-openedge-<version>.jar
    ├── openedge.jar
    └── pool.jar
```

Use the companion JARs required by the exact DataDirect/OpenEdge release you have installed. Do not put the driver JARs directly in the top-level `plugins/` directory as separate plugins. This approach uses the normal plugin classloader and does not require a Kestra Enterprise feature.

If the driver is missing, does not expose the JDBC service-provider entry, or one of its required classes cannot be linked, the task fails with an actionable `SQLException` identifying the required driver JARs.

Official references:

- [Progress: Connect from a Java application using a URL](https://docs.progress.com/bundle/openedge-sql-development/page/Connect-from-a-Java-application-using-a-URL.html)
- [Progress: Configure the JDBC driver](https://docs.progress.com/bundle/openedge-sql-quickstart/page/Configure-the-JDBC-driver.html)
- [Progress: DataDirect JDBC driver registration](https://docs.progress.com/bundle/datadirect-connect-jdbc-51/page/two.-Registering-the-Drivers.html)
- [Progress: Supported OpenEdge SQL data types](https://docs.progress.com/bundle/openedge-sql-reference/page/Supported-data-types.html)

## Connection

Use a DataDirect OpenEdge URL:

```text
jdbc:datadirect:openedge://<host>:<port>;databaseName=<database>
```

Credentials are provided through `username` and `password`. Use Kestra secrets for sensitive values.

## Query example

```yaml
id: openedge_query
namespace: company.team

tasks:
  - id: extract
    type: io.kestra.plugin.jdbc.openedge.Query
    url: jdbc:datadirect:openedge://erp-db.example.com:20000;databaseName=erp
    username: "{{ secret('OPENEDGE_USERNAME') }}"
    password: "{{ secret('OPENEDGE_PASSWORD') }}"
    sql: SELECT TOP 100 * FROM PUB.customer
    fetchType: STORE
```

## Copy OpenEdge data into PostgreSQL

```yaml
id: openedge_to_postgres
namespace: company.team

tasks:
  - id: extract
    type: io.kestra.plugin.jdbc.openedge.Query
    url: jdbc:datadirect:openedge://erp-db.example.com:20000;databaseName=erp
    username: "{{ secret('OPENEDGE_USERNAME') }}"
    password: "{{ secret('OPENEDGE_PASSWORD') }}"
    sql: SELECT custnum, name, balance FROM PUB.customer
    fetchType: STORE

  - id: load
    type: io.kestra.plugin.jdbc.postgresql.Batch
    from: "{{ outputs.extract.uri }}"
    url: jdbc:postgresql://warehouse:5432/analytics
    username: "{{ secret('POSTGRES_USERNAME') }}"
    password: "{{ secret('POSTGRES_PASSWORD') }}"
    sql: |
      INSERT INTO customer (custnum, name, balance)
      VALUES (?, ?, ?)
```

## Multiple statements

```yaml
id: openedge_queries
namespace: company.team

tasks:
  - id: queries
    type: io.kestra.plugin.jdbc.openedge.Queries
    url: jdbc:datadirect:openedge://erp-db.example.com:20000;databaseName=erp
    username: "{{ secret('OPENEDGE_USERNAME') }}"
    password: "{{ secret('OPENEDGE_PASSWORD') }}"
    sql: SELECT COUNT(*) FROM PUB.customer; SELECT COUNT(*) FROM PUB.orders
    fetchType: FETCH
```

## Polling trigger

```yaml
id: openedge_trigger
namespace: company.team

tasks:
  - id: each
    type: io.kestra.plugin.core.flow.Loop
    values: "{{ trigger.rows }}"
    tasks:
      - id: log_row
        type: io.kestra.plugin.core.log.Log
        message: "{{ fromJson(item.value) }}"

triggers:
  - id: watch
    type: io.kestra.plugin.jdbc.openedge.Trigger
    interval: PT5M
    url: jdbc:datadirect:openedge://erp-db.example.com:20000;databaseName=erp
    username: "{{ secret('OPENEDGE_USERNAME') }}"
    password: "{{ secret('OPENEDGE_PASSWORD') }}"
    sql: SELECT * FROM PUB.orders WHERE status = 'NEW'
    fetchType: FETCH
```

## SQL and result types

OpenEdge SQL uses `SELECT TOP n` for limiting rows; tables are commonly exposed under the `PUB` schema. This integration is JDBC-only: calling ABL procedures or the Autonomous REST connector is outside its scope.

The DataDirect driver maps OpenEdge `DATE`, `TIME`, and `TIMESTAMP` to the corresponding JDBC types. OpenEdge ABL `DATETIME` is exposed through SQL as `TIMESTAMP`. The converter normalizes `TIME` to `LocalTime` and `TIMESTAMP`/`DATETIME` to `LocalDateTime`; other values are delegated to the shared JDBC converter. The driver maps timestamp-with-time-zone to character data, so it is preserved through the normal string conversion path.

LOBs and less common types should be verified against the exact DataDirect release and OpenEdge schema before production use.

## Verification status

The module has unit tests for URL scheme exposure, the missing-driver diagnostic, and the supported date/time conversion branches. The proprietary driver is not available in CI, so a live connection test requires a licensed OpenEdge environment and remains a manual QA step.
