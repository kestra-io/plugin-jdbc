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
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
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
import java.util.Properties;

import static io.kestra.core.utils.Rethrow.throwBiConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute multiple SQL statements against a Microsoft Access database",
    description = """
        Executes multiple SQL statements sequentially against a Microsoft Access database (`.mdb` or `.accdb`),
        optionally within a single transaction.

        The database can be:
        - reused from a previous task via `accessFile`,
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
            title = "Execute multiple queries using an existing Access file and pass the results to another task.",
            code = """
                id: access_queries
                namespace: company.team

                tasks:
                  - id: init_db
                    type: io.kestra.plugin.jdbc.access.Queries
                    url: jdbc:ucanaccess:///myfile.accdb
                    outputDbFile: true
                    sql: |
                      CREATE TABLE IF NOT EXISTS products (
                        product_id INTEGER,
                        product_name TEXT
                      );
                      INSERT INTO products (product_id, product_name) VALUES (1, 'Widget');
                    fetchType: NONE

                  - id: select
                    type: io.kestra.plugin.jdbc.access.Queries
                    url: jdbc:ucanaccess:///myfile.accdb
                    accessFile: "{{ outputs.init_db.databaseUri }}"
                    sql: SELECT * FROM products
                    fetchType: FETCH
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
public class Queries extends AbstractJdbcQueries implements AccessQueryInterface {

    @Builder.Default
    @PluginProperty(group = "connection")
    @Schema(
        title = "The JDBC URL to connect to the database.",
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

    @Getter(AccessLevel.NONE)
    protected transient Path workingDirectory;

    @Getter(AccessLevel.NONE)
    protected transient Path databaseFile;

    @Override
    public Properties connectionProperties(RunContext runContext) throws Exception {
        var props = super.connectionProperties(runContext);

        if (this.databaseFile != null) {
            props.put("jdbc.url", "jdbc:ucanaccess://" + this.databaseFile.toAbsolutePath());
        }

        return AccessQueryUtils.buildAccessProperties(props, runContext);
    }

    private static String extractDbPath(String jdbcUrl) {
        if (jdbcUrl == null) {
            return "";
        }
        return jdbcUrl.replaceFirst("^jdbc:ucanaccess://", "");
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        this.workingDirectory = runContext.workingDir().path();
        additionalVars.put("workingDir", this.workingDirectory.toAbsolutePath().toString());

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
                fileName = Path.of(dbPath).getFileName().toString();
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

        var multiQueryOutput = super.run(runContext);

        URI dbUri = null;
        if (rOutputDbFile && this.databaseFile != null) {
            if (!Files.exists(this.databaseFile)) {
                Files.createFile(this.databaseFile);
            }
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
            .outputs(multiQueryOutput.getOutputs())
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output extends AbstractJdbcQueries.MultiQueryOutput {
        @Schema(title = "The output files' URI in Kestra's internal storage.")
        @PluginProperty(additionalProperties = URI.class)
        private final Map<String, URI> outputFiles;

        @Schema(title = "The database output URI in Kestra's internal storage.")
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
