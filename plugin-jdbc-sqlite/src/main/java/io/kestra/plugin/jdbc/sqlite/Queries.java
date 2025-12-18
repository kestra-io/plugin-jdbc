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
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
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
    title = "Run multiple SQLite queries.",
    description = """
        Executes multiple SQL statements sequentially against a SQLite database,
        optionally within a single transaction.

        The database can be:
        - reused from a previous task via `sqliteFile`,
        - referenced directly through the JDBC URL,
        - or created and persisted when `outputDbFile` is enabled.

        When `outputDbFile` is set to `true`, the database used during execution
        is uploaded to Kestra internal storage and exposed as `outputs.<taskId>.databaseUri`,
        enabling database reuse across tasks.
        """
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Execute multiple queries, using existing SQLite file, and pass the results to another task.",
            code = """
                id: sqlite_query_using_file
                namespace: company.team

                tasks:
                  - id: init_db
                    type: io.kestra.plugin.jdbc.sqlite.Queries
                    url: jdbc:sqlite:myfile.db
                    outputDbFile: true
                    sql: |
                      CREATE TABLE IF NOT EXISTS pgsql_types (
                        play_time TEXT,
                        concert_id INTEGER,
                        timestamp_type TEXT
                      );
                      INSERT INTO pgsql_types (play_time, concert_id, timestamp_type) VALUES ('2024-01', 1, '2024-01-01T12:00:00');

                  - id: select
                    type: io.kestra.plugin.jdbc.sqlite.Queries
                    url: jdbc:sqlite:myfile.db
                    sqliteFile: "{{ outputs.init_db.databaseUri }}"
                    outputDbFile: true
                    sql: select * from pgsql_types
                    fetchType: FETCH

                  - id: use_fetched_data
                    type: io.kestra.plugin.jdbc.sqlite.Queries
                    url: jdbc:sqlite:myfile.db
                    sqliteFile: "{{ outputs.select.databaseUri }}"
                    sql: |
                        CREATE TABLE IF NOT EXISTS pl_store_distribute (
                          year_month TEXT,
                          store_code INTEGER,
                          update_date TEXT
                        );
                        {% for row in outputs.select.outputs[0].rows %}
                            INSERT INTO pl_store_distribute (year_month, store_code, update_date)
                            VALUES ('{{row.play_time}}', {{row.concert_id}}, '{{row.timestamp_type}}');
                        {% endfor %}
                """
        )
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
public class Queries extends AbstractJdbcQueries implements SqliteQueryInterface {

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

        // If we decided to use a file-backed DB in working dir, force JDBC to that exact file
        if (this.databaseFile != null) {
            props.put("jdbc.url", "jdbc:sqlite:" + this.databaseFile.toAbsolutePath());
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

            // If URL targets an in-memory DB, generate a temp file name so we can persist/upload it.
            if (isMemoryDb(dbPath)) {
                fileName = runContext.workingDir()
                    .createTempFile(".sqlite")
                    .getFileName()
                    .toString();
            } else {
                fileName = Path.of(dbPath).getFileName().toString();
            }

            this.databaseFile = workingDirectory.resolve(fileName).normalize();

            // If sqliteFile is provided, download it under the expected fileName in working directory
            if (rSqliteFile.isPresent()) {
                PluginUtilsService.createInputFiles(
                    runContext,
                    workingDirectory,
                    Map.of(fileName, rSqliteFile.get()),
                    additionalVars
                );
            }
        } else {
            // Keep default behavior (e.g., test harness DBs)
            this.databaseFile = null;
        }

        AbstractJdbcQueries.MultiQueryOutput multiQueryOutput = super.run(runContext);

        URI dbUri = null;
        if (rOutputDbFile && this.databaseFile != null) {
            // Ensure file exists before upload (SQLite may create lazily depending on operations)
            if (!Files.exists(this.databaseFile)) {
                Files.createFile(this.databaseFile);
            }

            dbUri = runContext.storage().putFile(
                this.databaseFile.toFile(),
                this.databaseFile.getFileName().toString()
            );
        }

        return Output.builder()
            .databaseUri(dbUri)
            .outputs(multiQueryOutput.getOutputs())
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output extends AbstractJdbcQueries.MultiQueryOutput {
        @Schema(
            title = "The database output URI in Kestra's internal storage."
        )
        @PluginProperty
        private final URI databaseUri;
    }

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new SqliteCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
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
