package io.kestra.plugin.jdbc.sqlite;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcBaseQuery;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.sqlite.JDBC;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute a single SQL query against SQLite",
    description = """
        Executes a single SQL query against a SQLite database.

        The database can be:
        - referenced directly via the JDBC URL,
        - loaded from an existing SQLite file using `sqliteFile`,
        - or created on the fly when `outputDbFile` is enabled.

        When `outputDbFile` is set to `true`, the database file effectively used during execution
        is persisted to Kestra internal storage and exposed as `outputs.<taskId>.databaseUri`,
        allowing it to be reused by subsequent tasks.
        """
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Execute a query and pass the results to another task.",
            code = """
                id: sqlite_query
                namespace: company.team

                tasks:
                  - id: create_table
                    type: io.kestra.plugin.jdbc.sqlite.Query
                    url: jdbc:sqlite:myfile.db
                    outputDbFile: true
                    sql: |
                      CREATE TABLE IF NOT EXISTS pgsql_types (
                          concert_id INTEGER,
                          available INTEGER,
                          a TEXT,
                          b TEXT,
                          c TEXT,
                          d TEXT,
                          play_time TEXT,
                          library_record TEXT,
                          floatn_test REAL,
                          double_test REAL,
                          real_test REAL,
                          numeric_test NUMERIC,
                          date_type DATE,
                          time_type TIME,
                          timez_type DATETIME,
                          timestamp_type DATETIME,
                          timestampz_type DATETIME,
                          interval_type TEXT,
                          pay_by_quarter TEXT,
                          schedule TEXT,
                          json_type TEXT,
                          blob_type BLOB
                      );
                    fetchType: NONE

                  - id: select
                    type: io.kestra.plugin.jdbc.sqlite.Query
                    url: jdbc:sqlite:myfile.db
                    sqliteFile: "{{ outputs.create_table.databaseUri }}"
                    outputDbFile: true
                    sql: |
                      SELECT concert_id, available, a, b, c, d, play_time, library_record, floatn_test, double_test, real_test, numeric_test, date_type, time_type, timez_type, timestamp_type, timestampz_type, interval_type, pay_by_quarter, schedule, json_type, blob_type FROM pgsql_types;
                    fetchType: FETCH

                  - id: iterate_and_insert
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ outputs.select.rows }}"
                    tasks:
                      - id: insert_row
                        type: io.kestra.plugin.jdbc.sqlite.Query
                        url: jdbc:sqlite:myfile.db
                        sqliteFile: "{{ outputs.select.databaseUri }}"
                        sql: |
                          INSERT INTO pl_store_distribute (year_month, store_code, update_date)
                          VALUES ('{{ taskrun.value.play_time }}', {{ taskrun.value.concert_id }}, '{{ taskrun.value.timestamp_type }}');
                        fetchType: NONE
                """
        ),
    },
    metrics = {
        @Metric(
            name = "fetch.size",
            type = Counter.TYPE,
            unit = "rows",
            description = "The number of fetched rows."
        )
    }
)
public class Query extends AbstractJdbcQuery implements SqliteQueryInterface {

    @Builder.Default
    @PluginProperty(group = "connection")
    @Schema(
        title = "The JDBC URL to connect to the database.",
        description = "Example: `jdbc:sqlite:mydb.sqlite`",
        defaultValue = "jdbc:sqlite:"
    )
    private Property<String> url = Property.ofValue("jdbc:sqlite:");

    @PluginProperty(group = "connection")
    @Schema(
        title = "SQLite database file (optional)",
        description = """
            Optional URI to an existing SQLite database file stored in Kestra internal storage.

            When provided, the file is downloaded into the task working directory and used
            as the SQLite database for the query execution.
            """
    )
    protected Property<String> sqliteFile;

    @Schema(
        title = "Output the SQLite database file",
        description = """
            When set to `true`, the SQLite database file used during execution
            is uploaded to Kestra internal storage and exposed as `outputs.<taskId>.databaseUri`.
            """,
        defaultValue = "false"
    )
    @Builder.Default
    protected Property<Boolean> outputDbFile = Property.ofValue(false);

    @Getter(AccessLevel.NONE)
    protected transient Path workingDirectory;

    @Getter(AccessLevel.NONE)
    protected transient Path databaseFile;

    @Override
    public Properties connectionProperties(RunContext runContext) throws Exception {
        Properties props = super.connectionProperties(runContext);

        if (this.databaseFile != null) {
            props.put(
                "jdbc.url",
                "jdbc:sqlite:" + this.databaseFile.toAbsolutePath()
            );
        }

        return SqliteQueryUtils.buildSqliteProperties(props, runContext);
    }

    private static String extractDbPath(String jdbcUrl) {
        return jdbcUrl == null ? "" : jdbcUrl.replaceFirst("^jdbc:sqlite:", "");
    }

    private static boolean isMemoryDb(String dbPath) {
        if (dbPath == null) {
            return true;
        }
        String p = dbPath.trim().toLowerCase();
        return p.isEmpty()
            || p.equals(":memory:")
            || p.startsWith("file::memory:")
            || (p.startsWith("file:") && p.contains("mode=memory"));
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        this.workingDirectory = runContext.workingDir().path();

        Optional<String> rSqliteFile = runContext.render(this.sqliteFile).as(String.class);
        boolean rOutputDbFile = runContext.render(this.outputDbFile).as(Boolean.class).orElse(false);

        String rUrl = runContext.render(getUrl()).as(String.class).orElseThrow();
        String dbPath = extractDbPath(rUrl);

        boolean needsFileDb = rSqliteFile.isPresent() || rOutputDbFile;

        if (needsFileDb) {
            String fileName;

            if (isMemoryDb(dbPath)) {
                fileName = runContext.workingDir()
                    .createTempFile(".sqlite")
                    .getFileName()
                    .toString();
            } else {
                fileName = Path.of(dbPath).getFileName().toString();
            }

            this.databaseFile = workingDirectory.resolve(fileName).normalize();

            if (rSqliteFile.isPresent()) {
                PluginUtilsService.createInputFiles(
                    runContext,
                    workingDirectory,
                    Map.of(fileName, rSqliteFile.get()),
                    additionalVars
                );
            }
        } else {
            this.databaseFile = null;
        }

        AbstractJdbcBaseQuery.Output queryOutput = super.run(runContext);

        URI dbUri = null;

        if (rOutputDbFile && this.databaseFile != null && Files.exists(this.databaseFile)) {
            dbUri = runContext.storage().putFile(
                this.databaseFile.toFile(),
                this.databaseFile.getFileName().toString()
            );
        }

        return Output.builder()
            .databaseUri(dbUri)
            .row(queryOutput.getRow())
            .rows(queryOutput.getRows())
            .size(queryOutput.getSize())
            .uri(queryOutput.getUri())
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output extends AbstractJdbcQuery.Output {
        @Schema(title = "The database output URI in Kestra's internal storage.")
        @PluginProperty
        private final URI databaseUri;
    }

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new SqliteCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        if (DriverManager.drivers().noneMatch(JDBC.class::isInstance)) {
            DriverManager.registerDriver(new JDBC());
        }
    }

    @Override
    protected Integer getFetchSize(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.fetchSize).as(Integer.class).orElse(10000);
    }

    @Override
    public Property<String> getUrl() {
        return this.url;
    }
}
