package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.utils.IdUtils;
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
import java.util.stream.Collectors;

import jakarta.validation.constraints.NotNull;
import org.duckdb.DuckDBDriver;

import static io.kestra.core.utils.Rethrow.throwBiConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query a DuckDB database."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Query multiple CSV files from a ZIP file, include the filename in the result, and output both the final dataset in ION format and the DuckDB database file.",
            code = """
                id: query_multiple_csv_files
                namespace: company.team

                tasks:
                  - id: get_zip_file
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/resolve/main/zip/2023-01.zip

                  - id: unzip
                    type: io.kestra.plugin.compress.ArchiveDecompress
                    algorithm: ZIP
                    from: "{{outputs.get_zip_file.uri}}"

                  - id: duckdb
                    type: io.kestra.plugin.jdbc.duckdb.Query
                    inputFiles: "{{outputs.unzip.files}}"
                    sql: SELECT * FROM read_csv_auto('**/*-outcomes.csv', union_by_name=true, filename=true);
                    store: true  # output data in ION format
                    outputDbFile: true  # output the DuckDB database file
                """
        ),
        @Example(
            full = true,
            title = "Execute a query that reads a CSV file, and outputs another CSV file.",
            code = """
                id: query_duckdb
                namespace: company.team

                tasks:
                  - id: http_download
                    type: io.kestra.plugin.core.http.Download
                    uri: "https://huggingface.co/datasets/kestra/datasets/raw/main/csv/orders.csv"

                  - id: query
                    type: io.kestra.plugin.jdbc.duckdb.Query
                    url: 'jdbc:duckdb:'
                    timeZoneId: Europe/Paris
                    sql: |-
                      CREATE TABLE new_tbl AS SELECT * FROM read_csv_auto('data.csv', header=True);

                      COPY (SELECT order_id, customer_name FROM new_tbl) TO '{{ outputFiles.out }}' (HEADER, DELIMITER ',');
                    inputFiles:
                      data.csv: "{{ outputs.http_download.uri }}"
                    outputFiles:
                       - out
                """
        ),
        @Example(
            full = true,
            title = "Execute a query that reads from an existing database file using a URL.",
            code = """
                id: query_duckdb
                namespace: company.team

                inputs:
                  - id: my_db
                    type: FILE

                tasks:
                  - id: query1
                    type: io.kestra.plugin.jdbc.duckdb.Query
                    databaseUri: "{{ inputs.my_db }}"
                    sql: SELECT * FROM table_name;
                    fetchType: STORE
                """
        ),
        @Example(
            full = true,
            title = "Run a SQL query with DuckDB on MotherDuck and get the result as a CSV file",
            code = """
                    id: motherduck
                    namespace: company.team

                    tasks:
                      - id: query
                        type: io.kestra.plugin.jdbc.duckdb.Query
                        sql: |
                          SELECT by, COUNT(*) as nr_comments
                          FROM sample_data.hn.hacker_news
                          GROUP BY by
                          ORDER BY nr_comments DESC;
                        fetchType: STORE

                      - id: csv
                        type: io.kestra.plugin.serdes.csv.IonToCsv
                        from: "{{ outputs.query.uri }}"

                    pluginDefaults:
                      - type: io.kestra.plugin.jdbc.duckdb.Query
                        values:
                          url: jdbc:duckdb:md:my_db?motherduck_token={{ secret('MOTHERDUCK_TOKEN') }}
                          timeZoneId: Europe/Berlin
                """
        )
    }
)
public class Query extends AbstractJdbcQuery implements DuckDbQueryInterface {
    private static final String DEFAULT_URL = "jdbc:duckdb:";

    @Builder.Default
    private Property<String> url = Property.of(DEFAULT_URL);

    protected Object inputFiles;
    protected Property<List<String>> outputFiles;
    protected Property<String> databaseUri;

    @Builder.Default
    protected Property<Boolean> outputDbFile = Property.of(false);

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

    @Getter(AccessLevel.NONE)
    private transient Path databaseFile;

    @Override
    public Query.Output run(RunContext runContext) throws Exception {
        Path workingDirectory = null;

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
            }
        } else if (DEFAULT_URL.equals(renderedUrl) && this.databaseFile != null) {
            workingDirectory = databaseFile.toAbsolutePath().getParent();

            additionalVars.put("dbFilePath", databaseFile.toAbsolutePath());
        } else {
            this.databaseFile = runContext.workingDir().createTempFile(".db");
            Files.delete(this.databaseFile);
            workingDirectory = databaseFile.getParent();
        }

        if (workingDirectory != null) {
            additionalVars.put("workingDir", workingDirectory.toAbsolutePath().toString());
        }

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

        //Read database from URI and put it into workingDir
        if (this.databaseUri != null) {
            String dbName = IdUtils.create();
            PluginUtilsService.createInputFiles(
                runContext,
                workingDirectory,
                Map.of(dbName, runContext.render(this.databaseUri).as(String.class).orElseThrow()),
                additionalVars
            );
            this.databaseFile = Path.of(workingDirectory + "/" + dbName);
            this.url = new Property<>(DEFAULT_URL + this.databaseFile.toAbsolutePath());
        }

        // outputFiles
        var renderedOutputFiles = runContext.render(this.outputFiles).asList(String.class);
        if (!renderedOutputFiles.isEmpty()) {
            outputFiles = PluginUtilsService.createOutputFiles(
                workingDirectory,
                renderedOutputFiles,
                additionalVars
            );
        }

        final var configureFileSearchPathQuery = "SET file_search_path='" + workingDirectory + "';";
        String sql = this.sql.toString();

        // we transform filename column to show relative paths if filename parameter is used
        if (workingDirectory != null && sql.toLowerCase().matches(".*filename\\s*=\\s*(true|1).*")) {
            String escapedWorkingDir = workingDirectory.toString().replace("\\", "\\\\").replace("/", "\\/");
            sql = "SELECT * REPLACE(regexp_replace(filename, '^" + escapedWorkingDir + "/', '') AS filename) FROM (" +
                sql.replaceAll(";\\s*$", "") + ")";
        }

        this.sql = new Property<>(configureFileSearchPathQuery + "\n" + sql);

        AbstractJdbcQuery.Output run = super.run(runContext);

        // upload output files
        Map<String, URI> uploaded = new HashMap<>();

        if (outputFiles != null) {
            outputFiles
                .forEach(throwBiConsumer((k, v) -> uploaded.put(k, runContext.storage().putFile(new File(runContext.render(v, additionalVars))))));
        }

        //Create and output DB URI
        URI dbUri = null;
        if (Boolean.TRUE.equals(runContext.render(this.getOutputDbFile()).as(Boolean.class).orElseThrow())) {
            dbUri = runContext.storage().putFile(new File(this.databaseFile.toUri()));
        }

        return Output.builder()
            .row(run.getRow())
            .rows(run.getRows())
            .uri(run.getUri())
            .size(run.getSize())
            .outputFiles(uploaded)
            .databaseUri(dbUri)
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

        @Schema(
            title = "The database output URI in Kestra's internal storage."
        )
        @PluginProperty
        private final URI databaseUri;
    }

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
}
