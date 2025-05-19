package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.plugin.jdbc.AutoCommitInterface;
import io.micronaut.http.uri.UriBuilder;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;

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

import jakarta.validation.constraints.NotNull;
import org.duckdb.DuckDBDriver;

import static io.kestra.core.utils.Rethrow.throwBiConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query a DuckDb Database."
)
@Plugin(
    examples = {
        @Example(
            title = "Execute a query that reads a csv, and outputs another csv.",
            full = true,
            code = {
                "id: query-duckdb",
                "namespace: company.team",
                "tasks:",
                "  - id: http_download",
                "    type: io.kestra.plugin.core.http.Download",
                "    uri: \"https://huggingface.co/datasets/kestra/datasets/raw/main/csv/orders.csv\"",
                "  - id: query",
                "    type: io.kestra.plugin.jdbc.duckdb.Query",
                "    url: 'jdbc:duckdb:'",
                "    timeZoneId: Europe/Paris",
                "    sql: |-",
                "      CREATE TABLE new_tbl AS SELECT * FROM read_csv_auto('{{ workingDir }}/in.csv', header=True);",
                "",
                "      COPY (SELECT order_id, customer_name FROM new_tbl) TO '{{ outputFiles.out }}' (HEADER, DELIMITER ',');",
                "    inputFiles:",
                "      in.csv: \"{{ outputs.http_download.uri }}\"",
                "    outputFiles:",
                "       - out"
            }
        ),
        @Example(
            title = "Execute a query that reads from an existing database file using a URL.",
            full = true,
            code = {
                "id: query-duckdb",
                "namespace: company.team",
                "tasks:",
                "  - id: query1",
                "    type: io.kestra.plugin.jdbc.duckdb.Query",
                "    url: jdbc:duckdb:/{{ vars.dbfile }}",
                "    sql: SELECT * FROM table_name;",
                "    store: true",
                "",
                "  - id: query2",
                "    type: io.kestra.plugin.jdbc.duckdb.Query",
                "    url: jdbc:duckdb:/temp/folder/duck.db",
                "    sql: SELECT * FROM table_name;",
                "    store: true"
            }
        ),
        @Example(
            title = "Execute a query that reads from an existing database file using the `databaseFile` variable.",
            full = true,
            code = {
                "id: query-duckdb",
                "namespace: company.team",
                "tasks:",
                "  - id: query1",
                "    type: io.kestra.plugin.jdbc.duckdb.Query",
                "    url: jdbc:duckdb:",
                "    databaseFile:{{ vars.dbfile }}",
                "    sql: SELECT * FROM table_name;",
                "    store: true",
                "",
                "  - id: query2",
                "    type: io.kestra.plugin.jdbc.duckdb.Query",
                "    url: jdbc:duckdb:",
                "    databaseFile:/temp/folder/duck.db",
                "    sql: SELECT * FROM table_name;",
                "    store: true"
            }
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<Query.Output>, AutoCommitInterface {
    private static final String DEFAULT_URL = "jdbc:duckdb:";

    protected final Boolean autoCommit = true;

    @Schema(
        title = "Input files to be loaded from DuckDb.",
        description = "Describe a files map that will be written and usable by DuckDb. " +
            "You can reach files using a `workingDir` variable, example: `SELECT * FROM read_csv_auto('{{ workingDir }}/myfile.csv');` "
    )
    @PluginProperty(
        additionalProperties = String.class,
        dynamic = true
    )
    protected Object inputFiles;

    @Schema(
        title = "Output file list that will be uploaded to internal storage.",
        description = "List of keys that will generate temporary files.\n" +
            "On the SQL query, you can just use a variable named `outputFiles.key` for the corresponding file.\n" +
            "If you add a file with `[\"first\"]`, you can use the special vars `COPY tbl TO '{{ outputFiles.first }}' (HEADER, DELIMITER ',');`" +
            " and use this file in others tasks using `{{ outputs.taskId.outputFiles.first }}`."
    )
    @PluginProperty
    protected List<String> outputFiles;

    @Getter(AccessLevel.NONE)
    private transient Path databaseFile;

    @Builder.Default
    @PluginProperty(dynamic = true)
    private String url = DEFAULT_URL;

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new DuckDbCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(DuckDBDriver.class::isInstance)) {
            DriverManager.registerDriver(new DuckDBDriver());
        }
    }

    @Override
    @Schema(
        title = "The JDBC URL to connect to the database.",
        description = "The default value, `jdbc:duckdb:`, will use a local in-memory database. \nSet this property when connecting to a persisted database instance, for example `jdbc:duckdb:md:my_database?motherduck_token=<my_token>` to connect to [MotherDuck](https://motherduck.com/).",
        defaultValue = DEFAULT_URL
    )
    @NotNull
    public String getUrl() {
        if (DEFAULT_URL.equals(this.url)) {
            return DEFAULT_URL + this.databaseFile;
        }
        return this.url;
    }

    @Override
    public Query.Output run(RunContext runContext) throws Exception {
        Path workingDirectory;

        Map<String, String> outputFiles = null;

        if (!DEFAULT_URL.equals(this.url) && this.databaseFile == null) {
            String filePath = this.url.replace("jdbc:duckdb:", "");

            Path path = Path.of(filePath);
            if (path.isAbsolute()) {

                if (!Files.exists(path.getParent())) {
                    Files.createDirectory(path.getParent());
                }

                workingDirectory = path.getParent();
                this.databaseFile = path;

                additionalVars.put("dbFilePath", this.databaseFile.toAbsolutePath());

                URI url = URI.create(databaseFile.toAbsolutePath().toString());

                UriBuilder builder = UriBuilder.of(url);

                builder.scheme("jdbc:duckdb");

                this.url = builder.build().toString();
            } else {
                throw new IllegalArgumentException("The database file path is not valid (Path to database file must be absolute)");
            }
        } else if (DEFAULT_URL.equals(this.url) && this.databaseFile != null) {
            workingDirectory = databaseFile.toAbsolutePath().getParent();

            additionalVars.put("dbFilePath", databaseFile.toAbsolutePath());
        } else {
            this.databaseFile = runContext.workingDir().createTempFile(".db");
            Files.delete(this.databaseFile);
            workingDirectory = databaseFile.getParent();
        }

        additionalVars.put("workingDir", workingDirectory.toAbsolutePath().toString());

        // inputFiles
        if (this.inputFiles != null) {
            Map<String, String> finalInputFiles = PluginUtilsService.transformInputFiles(runContext, this.inputFiles);

            PluginUtilsService.createInputFiles(
                runContext,
                workingDirectory,
                finalInputFiles,
                additionalVars
            );
        }

        // outputFiles
        if (this.outputFiles != null && !this.outputFiles.isEmpty()) {
            outputFiles = PluginUtilsService.createOutputFiles(
                workingDirectory,
                this.outputFiles,
                additionalVars
            );
        }

        AbstractJdbcQuery.Output run = super.run(runContext);

        // upload output files
        Map<String, URI> uploaded = new HashMap<>();

        if (outputFiles != null) {
            outputFiles
                .forEach(throwBiConsumer((k, v) -> uploaded.put(k, runContext.storage().putFile(new File(runContext.render(v, additionalVars))))));
        }

        return Output.builder()
            .row(run.getRow())
            .rows(run.getRows())
            .uri(run.getUri())
            .size(run.getSize())
            .outputFiles(uploaded)
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output extends AbstractJdbcQuery.Output {
        @Schema(
            title = "The output files' URI in Kestra's internal storage."
        )
        @PluginProperty(additionalProperties = URI.class)
        private final Map<String, URI> outputFiles;
    }
}
