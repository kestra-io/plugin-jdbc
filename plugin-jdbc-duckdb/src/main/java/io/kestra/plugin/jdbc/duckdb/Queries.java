package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.micronaut.http.uri.UriBuilder;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.duckdb.DuckDBDriver;

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
    title = "Run multiple DuckDB queries."
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
                      CREATE TABLE new_tbl AS SELECT * FROM read_csv_auto('in.csv', header=True);
                      SELECT count(customer_name) FROM new_tbl;
                      SELECT customer_name FROM new_tbl;
                    inputFiles:
                      in.csv: "{{ outputs.http_download.uri }}"
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
                    type: io.kestra.plugin.jdbc.duckdb.Queries
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
public class Queries extends AbstractJdbcQueries implements DuckDbQueryInterface {
    private static final String DEFAULT_URL = "jdbc:duckdb:";

    protected Object inputFiles;

    protected Property<List<String>> outputFiles;

    protected Property<String> databaseUri;

    @Builder.Default
    protected Property<Boolean> outputDbFile = Property.ofValue(false);

    @Getter(AccessLevel.NONE)
    private transient Path databaseFile;

    @Builder.Default
    private Property<String> url = Property.ofValue(DEFAULT_URL);

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new DuckDbCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if it doesn't already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(DuckDBDriver.class::isInstance)) {
            DriverManager.registerDriver(new DuckDBDriver());
        }
    }

    @Override
    protected Integer getFetchSize(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.fetchSize).as(Integer.class).orElse(Integer.MIN_VALUE);
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
            return Property.ofValue(DEFAULT_URL + this.databaseFile);
        }
        return this.url;
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        Path workingDirectory;

        Map<String, String> outputFiles = null;

        var rUrl = runContext.render(this.url).as(String.class).orElseThrow();
        if (!DEFAULT_URL.equals(rUrl) && this.databaseFile == null) {
            String filePath = rUrl.replace("jdbc:duckdb:", "");

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

                this.url = Property.ofValue(builder.build().toString());
            } else {
                throw new IllegalArgumentException("The database file path is not valid (Path to database file must be absolute)");
            }
        } else if (DEFAULT_URL.equals(rUrl) && this.databaseFile != null) {
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

        if (this.inputFiles != null) {
            Map<String, String> finalInputFiles = PluginUtilsService.transformInputFiles(runContext, this.inputFiles);

            PluginUtilsService.createInputFiles(
                runContext,
                workingDirectory,
                finalInputFiles,
                additionalVars
            );
        }

        // Read database from URI and put it into workingDir
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

        var rOutputFiles = runContext.render(this.outputFiles).asList(String.class);
        if (!rOutputFiles.isEmpty()) {
            outputFiles = PluginUtilsService.createOutputFiles(
                workingDirectory,
                rOutputFiles,
                additionalVars
            );
        }

        final var configureFileSearchPathQuery = "SET file_search_path='" + workingDirectory + "';";
        this.sql = new Property<>(configureFileSearchPathQuery + "\n" + this.sql.toString());

        AbstractJdbcQueries.MultiQueryOutput run = super.run(runContext);

        // upload output files
        Map<String, URI> uploaded = new HashMap<>();

        if (outputFiles != null) {
            outputFiles
                .forEach(throwBiConsumer((k, v) -> uploaded.put(k, runContext.storage().putFile(new File(runContext.render(v, additionalVars))))));
        }

        // Create and output DB URI
        URI dbUri = null;
        if (runContext.render(this.getOutputDbFile()).as(Boolean.class).orElseThrow()) {
            dbUri = runContext.storage().putFile(new File(this.databaseFile.toUri()));
        }

        return Output.builder()
            .outputs(run.getOutputs())
            .outputFiles(uploaded)
            .databaseUri(dbUri)
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

        @Schema(
            title = "The database output URI in Kestra's internal storage."
        )
        @PluginProperty
        private final URI databaseUri;
    }

}
