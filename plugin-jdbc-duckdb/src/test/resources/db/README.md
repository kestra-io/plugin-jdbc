# Fixed legacy database fixtures

These resources were created with pinned legacy drivers, never the production
2.0 dependency. Keep the files fixed when updating that dependency.
`fixture_metadata.created_by` records the creating engine. The 1.4.5 and 1.5.6
fixtures contain two `legacy_events` rows. Tests copy each through internal
storage, modify it via `databaseUri`, export it with `outputDbFile`, reopen the
export in another task context, and verify the source still has two rows.

The 1.5.6 WAL pair checkpoints those two rows, commits a third row and terminates
without closing the connection. Its nonempty WAL must be replayed by 2.0; tests
copy both files before opening and also export/reopen the recovered database.
Never open the committed WAL pair in place.

| File | SHA-256 |
| --- | --- |
| `duckdb-1.4.5.db` | `ab12fc2c8b8d2580e1cc788e959337f00afd5720671b1138b3b17e331571e198` |
| `duckdb-1.5.6-wal.db` | `14e88feaca8afa194c5ed4fa54bd30dc967984f6f7d21084a3ab2b2ece2adefe` |
| `duckdb-1.5.6-wal.db.wal` | `1661669f134b3169c9a0275821e2578b800187c81001d2bc210cd6f290796c6d` |
| `duckdb-1.5.6.db` | `14e88feaca8afa194c5ed4fa54bd30dc967984f6f7d21084a3ab2b2ece2adefe` |

Fixture creators used `org.duckdb:duckdb_jdbc:1.4.5.0` and
`org.duckdb:duckdb_jdbc:1.5.6.0`, respectively. Each database records `version()`
in `fixture_metadata.created_by`. The schema is:

```sql
CREATE TABLE fixture_metadata AS SELECT version() AS created_by;
CREATE TABLE legacy_events (id INTEGER PRIMARY KEY, label VARCHAR);
INSERT INTO legacy_events VALUES (1, 'before upgrade'), (2, 'retained');
CHECKPOINT;
```

For the WAL pair, the creator then set `checkpoint_threshold='1GB'`, committed
`INSERT INTO legacy_events VALUES (3, 'recovered from WAL')` and terminated
without closing the connection. Keep these legacy fixtures fixed when the
production driver changes; regenerating them with 2.0 would remove the backward
compatibility coverage. Binary layouts can vary even with the same schema.
