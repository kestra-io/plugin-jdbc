DuckDB tasks run SQL directly inside DuckDB. You can query in-memory data, local files provided through `inputFiles`, and files or tables exposed by DuckDB extensions and functions.

Write standard DuckDB SQL in the task `sql` property. For self-contained workflows, a common pattern is to provide small datasets with `inputFiles` and query them directly from the task.

#### Bundled Ion extension

This plugin also bundles the DuckDB `ion` extension, so Amazon Ion files can be read with `read_ion(...)` without installing anything separately.

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

