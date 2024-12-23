package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AutoCommitInterface;
import io.micronaut.http.uri.UriBuilder;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

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

import static io.kestra.core.utils.Rethrow.throwBiConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Perform multiple queries to a DuckDb Database."
)
@Plugin(
    examples = {
        @Example(
            title = "Execute multiple queries that reads a csv, and outputs a select and a count.",
            full = true,
            code = """
                id: queries_duckdb
                namespace: company.team

                tasks:
                  - id: http_download
                    type: io.kestra.plugin.core.http.Download
                    uri: "https://huggingface.co/datasets/kestra/datasets/raw/main/csv/orders.csv"

                  - id: queries
                    type: io.kestra.plugin.jdbc.duckdb.Queries
                    url: 'jdbc:duckdb:'
                    timeZoneId: Europe/Paris
                    sql: |-
                      CREATE TABLE new_tbl AS SELECT * FROM read_csv_auto('{{ workingDir }}/in.csv', header=True);
                      SELECT count(customer_name) FROM new_tbl;
                      SELECT customer_name FROM new_tbl;
                    inputFiles:
                      in.csv: "{{ outputs.http_download.uri }}"
                """
        ),
        @Example(
            title = "Execute queries that reads from an existing database file using a URL.",
            full = true,
            code = """
                id: query_duckdb
                namespace: company.team

                tasks:
                  - id: query1
                    type: io.kestra.plugin.jdbc.duckdb.Query
                    url: jdbc:duckdb:/{{ vars.dbfile }}
                    sql: SELECT * FROM table1_name; SELECT * FROM table2_name;
                    fetchType: STORE

                  - id: query2
                    type: io.kestra.plugin.jdbc.duckdb.Query
                    url: jdbc:duckdb:/temp/folder/duck.db
                    sql: SELECT * FROM table1_name; SELECT * FROM table2_name;
                    fetchType: STORE
                """
        )
    }
)
public class Queries extends AbstractJdbcQueries implements RunnableTask<Queries.Output> {
    private static final String DEFAULT_URL = "jdbc:duckdb:";

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
    private Property<String> url = Property.of(DEFAULT_URL);

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new DuckDbCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new org.duckdb.DuckDBDriver());
    }

    @Override
    @Schema(
        title = "The JDBC URL to connect to the database.",
        description = "The default value, `jdbc:duckdb:`, will use a local in-memory database. \nSet this property when connecting to a persisted database instance, for example `jdbc:duckdb:md:my_database?motherduck_token=<my_token>` to connect to [MotherDuck](https://motherduck.com/).",
        defaultValue = DEFAULT_URL
    )
    @NotNull
    public Property<String> getUrl() {
        if (DEFAULT_URL.equals(this.url.toString())) {
            return Property.of(DEFAULT_URL + this.databaseFile);
        }
        return this.url;
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        Path workingDirectory;

        Map<String, String> outputFiles = null;

        var renderedUrl = runContext.render(this.url).as(String.class).orElseThrow();
        if (!DEFAULT_URL.equals(renderedUrl) && this.databaseFile == null) {
            String filePath = renderedUrl.replace("jdbc:duckdb:", "");

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

                this.url = Property.of(builder.build().toString());
            } else {
                throw new IllegalArgumentException("The database file path is not valid (Path to database file must be absolute)");
            }
        } else if (DEFAULT_URL.equals(renderedUrl) && this.databaseFile != null) {
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

        AbstractJdbcQueries.MultiQueryOutput run = super.run(runContext);

        // upload output files
        Map<String, URI> uploaded = new HashMap<>();

        if (outputFiles != null) {
            outputFiles
                .forEach(throwBiConsumer((k, v) -> uploaded.put(k, runContext.storage().putFile(new File(runContext.render(v, additionalVars))))));
        }

        return Output.builder()
            .outputs(run.getOutputs())
            .outputFiles(uploaded)
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output extends AbstractJdbcQueries.MultiQueryOutput {
        @Schema(
            title = "The output files' URI in Kestra's internal storage."
        )
        @PluginProperty(additionalProperties = URI.class)
        private final Map<String, URI> outputFiles;
    }
}
