Trino tasks run their statements on a Trino coordinator. Support includes querying catalogs such as `tpch`, `tpcds`, `memory` or `hive`, aggregations or transformations pushed down to Trino, and writes to catalogs that accept them.

Every task connects with a JDBC URL such as `jdbc:trino://host:8080/catalog/schema`. The catalog, schema, TLS, authentication, client and session settings can also be set as typed task properties instead.

#### Authentication

- `username` and `password`: password authentication. The driver refuses to send a password over plain HTTP, so set `ssl: true`.
- `accessToken`: a JWT, for example issued by an OAuth2/OIDC provider. Browser-based OAuth2 (`externalAuthentication`) is not supported on a headless worker.
- `sslKeyStore`, `sslKeyStorePassword` and `sslKeyStoreType`: mutual TLS with a client certificate.
- `kerberosPrincipal`, `kerberosRemoteServiceName`, `kerberosKeytab` and `kerberosConfig`: Kerberos.
- `sslTrustStore` and `sslVerification`: how the coordinator certificate is validated.

`password`, `accessToken`, `sslKeyStore`, both store passwords, `kerberosKeytab` and `extraCredentials` are secrets: pass them with [`secret()`](https://kestra.io/docs/concepts/secret), and set shared values once with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).

Keystores, truststores, the keytab and the krb5 configuration are declared as *content*: base64-encoded bytes or a `kestra://` internal storage URI. They are written to the task working directory and only their path is passed to the driver, so nothing has to be pre-installed on the worker.

The driver rejects a property set both in the URL and as a task property (except `ssl`, which overrides the URL). The task then fails with a message naming the property, so remove one of the two.

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

#### Tasks

- `Query` runs one statement. Its `queryId` output is the id the coordinator assigned to it.
- `Queries` runs several statements separated by semicolons. Each statement's `queryId` is in the `statements` file and in the matching entry of the `outputs` list.

Both log the query id with a link to its page in the Trino Web UI (`host:port/ui/query.html?<queryId>`), and emit the coordinator stats of each query as metrics: `processed.rows`, `processed.bytes`, `peak.memory.bytes`, `physical.written.bytes`, `cpu.time`, `wall.time` and `queued.time`. With the default `fetchType: NONE`, result sets are not read just to collect stats, so these metrics use the latest status available and may be incomplete. Use `FETCH` or `STORE` when you need the final stats.

The `source` property defaults to `kestra`, so the coordinator reports each query with it in `system.runtime.queries` and resource group selectors can match it. `clientTags` refines that routing.

By default the statements of `Queries` run outside a transaction (`transaction: false`), because catalogs such as `memory` accept writes in autocommit mode only. Set `transaction: true` to run them as one transaction, so a failure rolls the statements back, with catalogs that support it.

#### Triggers

`Trigger` runs a query on an interval and starts a flow when it returns rows. It accepts the same connection and authentication properties as the tasks.
