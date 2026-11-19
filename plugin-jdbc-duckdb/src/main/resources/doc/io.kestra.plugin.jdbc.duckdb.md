DuckDB tasks run SQL directly inside DuckDB. You can query in-memory data, local files provided through `inputFiles`, and files or tables exposed by DuckDB extensions and functions.

Write standard DuckDB SQL in the task `sql` property. For self-contained workflows, a common pattern is to provide small datasets with `inputFiles` and query them directly from the task.

#### Bundled Ion extension

This plugin bundles the DuckDB `ion` extension for Linux AMD64 and ARM64, so Amazon Ion files can be read with `read_ion(...)` without a network download on these platforms.

For DuckDB 2.0, the bundled files are loaded through a local repository pinned with the matching DuckDB release's community public keys. Signature validation stays enabled. Repository definitions, bundled source files and bundled installations are kept in the task's temporary directory. Bundled installation temporarily selects a task-local extension cache, then restores DuckDB's worker cache before user SQL runs. Community and autoloaded extensions retain the default cache, including pre-provisioned `~/.duckdb/extensions`, and can be reused across tasks. Repository changes are forbidden after the bundled installation. On other platforms, or if bundled loading fails, the plugin attempts to install Ion from the community repository and logs a warning if installation or loading fails.

This is useful in Kestra because some task outputs and internal data exchanges use Ion-compatible files, which can then be queried directly from DuckDB tasks.

For simple cases, schema inference works automatically:

```sql
SELECT *
FROM read_ion('sample.ion');
```

For nested or inconsistent Ion records, defining `columns := {...}` is recommended because it makes the schema explicit and avoids inference surprises:

```yaml
id: duckdb-ion
namespace: company.team

tasks:
  - id: query
    type: io.kestra.plugin.jdbc.duckdb.Query
    inputFiles:
      sample.ion: |
        {id: 1, nested: {name: "alpha"}, tags: ["x", "y"]}
        {id: 2, nested: {name: "beta"}, tags: ["z"]}
    sql: |
      SELECT id, nested.name, list_extract(tags, 1) AS first_tag
      FROM read_ion('sample.ion', columns := {
        id: 'BIGINT',
        nested: 'STRUCT(name VARCHAR)',
        tags: 'VARCHAR[]'
      })
      ORDER BY id
```

`read_ion` also supports globs and lists of files. When reading multiple files with varying schemas, `union_by_name := true` can help merge fields by name.

#### Automatic output files

Files generated within DuckDB queries in the task's working directory (such as via `COPY ... TO '{{ workingDir }}/results.csv'`) are automatically captured and uploaded to Kestra's internal storage as task outputs.

To reference these output files in downstream tasks, use bracket syntax:

```yaml
{{ outputs.taskId.outputFiles['results.csv'] }}
```

Nested directory paths (e.g., `COPY ... TO '{{ workingDir }}/exports/data.csv'`) are also supported and accessible via `{{ outputs.taskId.outputFiles['exports/data.csv'] }}`.

Automatic capture is enabled by default and can be opted out of by setting `captureOutputFiles: false`.

##### Exclusions and Limits

The following files are excluded from automatic capture:
- Database files (`*.db` and `*.wal`)
- Temporary files (`*.tmp`)
- Extension downloads in `.duckdb_extensions/`
- Pre-existing files that were not modified during query execution

Safety limits are enforced to prevent unintended large uploads:
- `maxCapturedFiles` (default: 100): Maximum number of new or modified files captured.
- `maxCapturedBytes` (default: 100 MB / 104,857,600 bytes): Maximum total size of captured files.

If either limit is exceeded, execution fails with an error explaining how to adjust the limits or disable auto-capture.

#### File paths

DuckDB resolves relative paths against the Kestra process directory, not the task working directory. To stay safe:

- Read input files with `'{{ workingDir }}/data.csv'`. A relative `'data.csv'` also works as long as no file of that name exists in the process directory; if one does, the task fails instead of silently reading it.
- Write files with `COPY ... TO '{{ workingDir }}/results.csv'` or `'{{ outputFiles.key }}'`. A relative `COPY ... TO 'results.csv'` lands in the process directory, is not captured as an output, and is visible to other executions. Kestra logs a warning when it detects such a write.

Write detection only scans the top level of the process directory, and it may report files written by other tasks running concurrently on the same worker.

```yaml
id: duckdb-paths
namespace: company.team

tasks:
  - id: query
    type: io.kestra.plugin.jdbc.duckdb.Query
    inputFiles:
      data.csv: |
        id,name
        1,alpha
    sql: |
      COPY (SELECT * FROM read_csv_auto('{{ workingDir }}/data.csv'))
      TO '{{ workingDir }}/results.csv' (HEADER, DELIMITER ',');
```

#### Upgrading to DuckDB 2.0

Review the [DuckDB migration guide](https://duckdb.org/docs/2.0/operations_manual/migration)
before upgrading existing flows.

- New database files use format 69, unreadable by 1.x. Existing files retain
  their format. Check all consumers of `outputDbFile` and keep original inputs
  for rollback. The plugin adds no `storageVersion` property; use an existing
  compatible file through `databaseUri` when sharing with older consumers.
- Replace `x -> x + 1` with `lambda x : x + 1`. Temporarily, use
  `SET lambda_syntax = 'enable_single_arrow'`.
- Integer `//` and `%` by zero throw. Use `TRY(...)` or
  `SET error_on_division_by_zero=false` for NULL results.
- Use DuckDB secrets for S3 credentials instead of URL parameters.
- Only bundled installations use a temporary task-local cache, populated from the
  shipped binary without a network download. The worker's extension cache is
  restored before user SQL, so pre-provisioned, community and autoloaded extensions
  remain reusable across tasks; httpfs is not downloaded again for each task.
- For parser regressions, use `INSTALL AND LOAD legacy_parser;` followed by
  `SET enable_legacy_parser = true;`.
- Review regex matching, factorial precedence, binary literals, transaction
  errors, duplicate-source MERGE operations, untyped NULLs, identifier case,
  and profiling/logging settings in the migration guide.

Use `Queries` for session setup followed by SQL; `afterSQL` runs after the query.
`VARIANT` results are exposed as maps, lists and scalar values in `FETCH`,
`FETCH_ONE` and `STORE`.

#### MotherDuck compatibility

Check [MotherDuck's supported client versions](https://motherduck.com/docs/troubleshooting/version-lifecycle-schedules/)
before upgrading a flow that uses `jdbc:duckdb:md:`. As of November 19, 2026,
MotherDuck lists DuckDB 1.5.6 as its maximum supported engine and JDBC 1.5.6.0
as its supported Java client. DuckDB 2.0 is outside that published range. Keep
MotherDuck workflows on a plugin release with a supported driver until
MotherDuck announces 2.0 support. The upgrade has no authenticated MotherDuck
validation.
