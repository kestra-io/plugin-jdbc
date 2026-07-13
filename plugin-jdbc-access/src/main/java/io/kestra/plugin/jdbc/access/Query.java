package io.kestra.plugin.jdbc.access;

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
import net.ucanaccess.jdbc.UcanaccessDriver;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.Properties;

import static io.kestra.core.utils.Rethrow.throwBiConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute a single SQL query against a Microsoft Access database",
    description = """
        Executes a single SQL query against a Microsoft Access database (`.mdb` or `.accdb`) via the UCanAccess pure-Java JDBC driver.

        The database can be:
        - referenced directly via the JDBC URL,
        - loaded from an existing Access file using `accessFile`,
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
                id: access_query
                namespace: company.team

                tasks:
                  - id: create_table
                    type: io.kestra.plugin.jdbc.access.Query
                    url: jdbc:ucanaccess:///myfile.accdb
                    outputDbFile: true
                    sql: |
                      CREATE TABLE IF NOT EXISTS products (
                        product_id INTEGER,
                        product_name TEXT,
                        price DOUBLE,
                        available BIT,
                        created_date DATETIME
                      )
                    fetchType: NONE

                  - id: select
                    type: io.kestra.plugin.jdbc.access.Query
                    url: jdbc:ucanaccess:///myfile.accdb
                    accessFile: "{{ outputs.create_table.databaseUri }}"
                    outputDbFile: true
                    sql: SELECT * FROM products
                    fetchType: FETCH
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
public class Query extends AbstractJdbcQuery implements AccessQueryInterface {

    @Builder.Default
    @PluginProperty(group = "connection")
    @Schema(
        title = "The JDBC URL to connect to the database",
        description = "Example: `jdbc:ucanaccess:///path/to/mydb.accdb`",
        defaultValue = "jdbc:ucanaccess:///"
    )
    private Property<String> url = Property.ofValue("jdbc:ucanaccess:///");

    @PluginProperty(group = "source")
    @Schema(
        title = "Access database file (optional)",
        description = """
            Optional URI to an existing Access database file stored in Kestra internal storage.

            When provided, the file is downloaded into the task working directory and used
            as the Access database for the query execution.
            """
    )
    protected Property<String> accessFile;

    @Schema(
        title = "Output the Access database file",
        description = """
            When set to `true`, the Access database file used during execution
            is uploaded to Kestra internal storage and exposed as `outputs.<taskId>.databaseUri`.
            """,
        defaultValue = "false"
    )
    @Builder.Default
    @PluginProperty(group = "source")
    protected Property<Boolean> outputDbFile = Property.ofValue(false);

    @Builder.Default
    @PluginProperty(group = "source")
    @Schema(
        title = "Access database version to use when creating a new database file",
        description = """
            Specifies the Microsoft Access file format used when UCanAccess creates a new database.
            Only applies when the target database file does not already exist.
            See https://spannm.github.io/ucanaccess/20-getting-started.html for details.
            """,
        defaultValue = "V2003"
    )
    private Property<AccessVersion> newDatabaseVersion = Property.ofValue(AccessVersion.V2003);

    @Getter(AccessLevel.NONE)
    protected transient Path workingDirectory;

    @Getter(AccessLevel.NONE)
    protected transient Path databaseFile;

    @Override
    public Properties connectionProperties(RunContext runContext) throws Exception {
        var props = super.connectionProperties(runContext);
        var version = runContext.render(this.newDatabaseVersion).as(AccessVersion.class).orElse(AccessVersion.V2003);

        if (this.databaseFile != null) {
            props.put("jdbc.url", "jdbc:ucanaccess://" + this.databaseFile.toAbsolutePath());
        }

        return AccessQueryUtils.buildAccessProperties(props, runContext, version);
    }

    private static String extractDbPath(String jdbcUrl) {
        if (jdbcUrl == null) {
            return "";
        }
        // jdbc:ucanaccess:///absolute/path → /absolute/path  or  jdbc:ucanaccess://relative → relative
        var after = jdbcUrl.replaceFirst("^jdbc:ucanaccess://", "");
        // triple-slash absolute path: strip leading slash since Path.of on "/path" works on Unix but we track filename
        return after;
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        this.workingDirectory = runContext.workingDir().path();

        var rAccessFile = runContext.render(this.accessFile).as(String.class);
        var rOutputDbFile = runContext.render(this.outputDbFile).as(Boolean.class).orElse(false);
        var rUrl = runContext.render(getUrl()).as(String.class).orElseThrow();
        var dbPath = extractDbPath(rUrl);

        var needsFileDb = rAccessFile.isPresent() || rOutputDbFile;

        if (needsFileDb) {
            String fileName;
            if (dbPath.isEmpty() || dbPath.equals("/")) {
                fileName = runContext.workingDir()
                    .createTempFile(".accdb")
                    .getFileName()
                    .toString();
            } else {
                // Use a UUID-based name so every task execution gets a unique absolute path.
                // This prevents DBReferenceSingleton from returning a stale HSQLDB DBReference
                // when two concurrent tasks happen to resolve the same user-supplied filename
                // (e.g. "myfile.accdb") to the same working directory.
                String ext = dbPath.toLowerCase().endsWith(".mdb") ? ".mdb" : ".accdb";
                fileName = UUID.randomUUID() + ext;
            }

            this.databaseFile = workingDirectory.resolve(fileName).normalize();

            if (rAccessFile.isPresent()) {
                PluginUtilsService.createInputFiles(
                    runContext,
                    workingDirectory,
                    Map.of(fileName, rAccessFile.get()),
                    additionalVars
                );
            }
        } else {
            this.databaseFile = null;
        }

        var rOutputFiles = this.outputFiles == null
            ? List.<String>of()
            : runContext.render(this.outputFiles).asList(String.class);
        Map<String, String> outputFilesMap = null;
        if (!rOutputFiles.isEmpty()) {
            outputFilesMap = PluginUtilsService.createOutputFiles(
                this.workingDirectory,
                rOutputFiles,
                additionalVars
            );
        }

        AbstractJdbcBaseQuery.Output queryOutput = super.run(runContext);

        URI dbUri = null;
        if (rOutputDbFile && this.databaseFile != null && Files.exists(this.databaseFile)) {
            dbUri = runContext.storage().putFile(
                this.databaseFile.toFile(),
                this.databaseFile.getFileName().toString()
            );
        }

        var uploaded = new HashMap<String, URI>();
        if (outputFilesMap != null) {
            outputFilesMap.forEach(throwBiConsumer((k, v) ->
                uploaded.put(k, runContext.storage().putFile(new File(runContext.render(v, additionalVars))))));
        }

        return Output.builder()
            .databaseUri(dbUri)
            .outputFiles(uploaded.isEmpty() ? null : uploaded)
            .row(queryOutput.getRow())
            .rows(queryOutput.getRows())
            .size(queryOutput.getSize())
            .uri(queryOutput.getUri())
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output extends AbstractJdbcQuery.Output {
        @Schema(title = "The output files' URI in Kestra's internal storage")
        @PluginProperty(additionalProperties = URI.class)
        private final Map<String, URI> outputFiles;

        @Schema(title = "The database output URI in Kestra's internal storage")
        @PluginProperty
        private final URI databaseUri;
    }

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new AccessCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        if (DriverManager.drivers().noneMatch(UcanaccessDriver.class::isInstance)) {
            DriverManager.registerDriver(new UcanaccessDriver());
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
