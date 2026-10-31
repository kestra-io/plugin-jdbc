Trino tasks run their statements on a Trino coordinator. Support includes querying catalogs such as `tpch`, `tpcds`, `memory` or `hive`, aggregations or transformations pushed down to Trino, and writes to catalogs that accept them.

Every task connects with a JDBC URL such as `jdbc:trino://host:8080/catalog/schema`; everything passed through the URL can also be set as a typed task property instead.

#### Connection details

Connection details can live in the JDBC URL or as typed properties — token or password authentication, default catalog and schema, TLS and client certificates, Kerberos, or session tuning:

```yaml
id: trino-typed-properties
namespace: company.team

tasks:
  - id: query_orders
    type: io.kestra.plugin.jdbc.trino.Query
    url: jdbc:trino://trino.example.com:8443
    ssl: true
    username: "{{ secret('TRINO_USERNAME') }}"
    password: "{{ secret('TRINO_PASSWORD') }}"
    catalog: tpch
    schema: tiny
    clientTags:
      - reported_by_kestra
    sql: |
      SELECT orderkey, totalprice FROM orders WHERE orderkey < 10
    fetchType: STORE
```

The `source` property defaults to `kestra` so the coordinator reports each query with it, visible in `system.runtime.queries` and usable in resource group selectors; `clientTags` refines that routing.

The Trino JDBC driver refuses a connection detail passed both by the URL and as a property (only `ssl` may be overridden): when both are set, the task fails early explaining which property is ambiguous, before any connection is attempted.

Keystore, truststore, Kerberos keytab and krb5 configuration are declared as *content*: base64-encoded bytes or a `kestra://` internal storage URI. They are materialized in the task working directory and only their path is passed to the driver, so nothing has to be pre-installed on the worker. Keep in mind that authenticating with a password requires TLS (`ssl: true`), as the driver refuses sending a password over plain HTTP.

#### Query identifiers and stats

Trino assigns each executed statement an id; the task returns it in the `queryId` of its outputs and logs it with the link to its detail page in the Trino Web UI (`host:port/ui/query.html?<queryId>`). The `Queries` task exposes one per statement: in the `statements` file alongside the duration and affected rows of each statement, and in the queryId of the matching entry of the `outputs` list.

The coordinator stats of each query are also emitted as task metrics: `processed.rows`, `processed.bytes`, `peak.memory.bytes`, `physical.written.bytes`, `cpu.time`, `wall.time` and `queued.time`, which lets you track what a workflow actually costs the engine.

#### Transactions

By default the statements of a `Queries` task run outside a transaction (`transaction: false`), because Trino requires writes to happen in autocommit mode. Set `transaction: true` to have the statements run as one transaction so a failure rolls the file back, with catalogs that support it.
