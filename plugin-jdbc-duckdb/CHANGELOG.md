# DuckDB release notes

## Unreleased — breaking upgrade to DuckDB 2.0

Release this upgrade as a major version, not a 3.1.x patch. The JDBC dependency
moves from 1.5.6.0 to 2.0.0.1 (engine v2.0.0); the Kestra dependency is unchanged.

### Database file compatibility

New databases use DuckDB 2.0 storage format 69 by default. DuckDB 1.x cannot
open these files, including after a plugin downgrade. Upgrade all readers, or
set `storageVersion` before creating a database that an older reader must open:

```yaml
id: duckdb-compatible-export
namespace: company.team

tasks:
  - id: export_database
    type: io.kestra.plugin.jdbc.duckdb.Query
    storageVersion: v1.5.6
    sql: CREATE TABLE events AS SELECT 42 AS id;
    fetchType: NONE
    outputDbFile: true
```

`storageVersion` is an optional rendered `Property<String>` on Query, Queries
and Trigger. Examples include `v1.4.5`, `v1.5.6`, `v2.0.0` and `latest`. It sets
DuckDB's `storage_compatibility_version` at connection startup, before the file
is created. Omitting the property preserves the engine default. DuckDB validates
version names and may reject features that cannot be persisted in an older format.

Existing files retain their format, including 1.4/1.5 files read through
`databaseUri`. Setting an older target does not downgrade an existing 2.0 file.
Keep original inputs for rollback; export/import into a newly created compatible
file if conversion is needed. This setting does not restore older SQL behavior.

### SQL migration

- Replace `x -> x + 1` lambdas with `lambda x : x + 1`. The temporary bridge is
  `SET lambda_syntax = 'enable_single_arrow'` and is planned for removal in 2.1.
- Integer `//` and `%` by zero now throw. Use `TRY(...)` or
  `SET error_on_division_by_zero=false` for NULL results.
- Use DuckDB secrets for S3 credentials; S3 URL credential parameters are disabled
  by default.
- The PEG parser is the default. For parser regressions, use
  `INSTALL AND LOAD legacy_parser; SET enable_legacy_parser=true;`.
- Review regex partial matching, factorial precedence, binary literals,
  multi-matched MERGE operations, untyped NULLs, identifier case and
  profiling/logging settings. Any error inside an explicit transaction now
  requires rollback before subsequent statements can run.

See the [DuckDB 2.0 migration guide](https://duckdb.org/docs/2.0/operations_manual/migration)
for the full SQL changes and their workarounds.

### MotherDuck

The published MotherDuck range checked on November 19, 2026 ends at DuckDB
1.5.6/JDBC 1.5.6.0. This upgrade is outside that supported range and has no
authenticated MotherDuck validation. Keep MotherDuck flows on a compatible
plugin release until MotherDuck supports the 2.0 client and it is validated.

Query, Queries and Trigger emit a runtime warning for rendered
`jdbc:duckdb:md:` URLs before connecting. The warning contains neither the URL
nor its token. `storageVersion` does not enable MotherDuck compatibility.

### Other behavior and output changes

- Bundled Ion uses signed, pinned community keys. Repository changes are locked
  after installation; SQL that changes `custom_extension_repository` or custom
  repository trust configuration afterward may fail.
- Bundled installation is task-local; the worker's extension cache is restored
  before user SQL so pre-provisioned, community and autoloaded extensions can be
  reused across tasks.
- VARIANT now returns structured maps/lists/scalars instead of opaque strings.
- Untyped NULL metadata changes from INTEGER to `"NULL"`.
- TIMESTAMPTZ_NS is a new type; its JDBC text is converted to UTC OffsetDateTime
  with nanoseconds preserved.
- Trigger now honors its configured/rendered URL. Previously it ignored that
  URL and always polled a temporary database. The default still uses a temporary
  local database through Query's lifecycle.
