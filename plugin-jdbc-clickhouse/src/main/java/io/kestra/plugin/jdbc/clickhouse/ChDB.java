package io.kestra.plugin.jdbc.clickhouse;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.*;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.runner.docker.Docker;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Embedded ClickHouse (chDB / clickhouse-local) query task for lightweight transforms.
 *
 * <p>Mirrors the chDB Python API surface: {@code query} + {@code outputFormat}, while
 * integrating with Kestra internal storage (Ion by default, dedicated extensions for file formats).
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query data with embedded ClickHouse (chDB)",
    description = """
        Runs a SQL query with an embedded ClickHouse engine (clickhouse-local / chDB) for lightweight \
        data transformations — no external ClickHouse server required.

        Main properties match the chDB API: `query` and optional `outputFormat`.

        Result storage:
        - When `outputFormat` is omitted (default tabular output) or a Pretty* format (e.g. `PrettyCompact`), \
        the result is stored as an Amazon Ion file (Kestra's intermediate tabular format).
        - When `outputFormat` is a file-oriented ClickHouse format (e.g. `CSVWithNames`, `Parquet`), \
        the result is stored with a matching file extension (`.csv`, `.parquet`, …).

        Supports reading remote URLs and local `inputFiles` via ClickHouse table functions such as \
        `url(...)`, `file(...)`, and `s3(...)`.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Aggregate a remote CSV with embedded ClickHouse and store the result as Ion",
            full = true,
            code = """
                id: chdb_query
                namespace: company.team

                tasks:
                  - id: transform
                    type: io.kestra.plugin.jdbc.clickhouse.ChDB
                    query: |
                      SELECT
                        sum(total) AS total,
                        avg(quantity) AS avg_quantity
                      FROM url('https://huggingface.co/datasets/kestra/datasets/raw/main/csv/orders.csv')
                """
        ),
        @Example(
            title = "Export query results as CSV (CSVWithNames → .csv artifact)",
            full = true,
            code = """
                id: chdb_csv
                namespace: company.team

                tasks:
                  - id: export
                    type: io.kestra.plugin.jdbc.clickhouse.ChDB
                    query: |
                      SELECT number, number * 2 AS doubled
                      FROM system.numbers
                      LIMIT 10
                    outputFormat: CSVWithNames
                """
        ),
        @Example(
            title = "Query local files passed via inputFiles",
            full = true,
            code = """
                id: chdb_local_file
                namespace: company.team

                inputs:
                  - id: orders
                    type: FILE

                tasks:
                  - id: summarize
                    type: io.kestra.plugin.jdbc.clickhouse.ChDB
                    inputFiles:
                      orders.csv: "{{ inputs.orders }}"
                    query: |
                      SELECT count() AS row_count
                      FROM file('orders.csv', 'CSVWithNames')
                """
        ),
        @Example(
            title = "PrettyCompact display format is normalized to Ion for Kestra storage",
            full = true,
            code = """
                id: chdb_pretty
                namespace: company.team

                tasks:
                  - id: preview
                    type: io.kestra.plugin.jdbc.clickhouse.ChDB
                    query: SELECT version() AS clickhouse_version
                    outputFormat: PrettyCompact
                """
        )
    }
)
public class ChDB extends Task implements RunnableTask<ChDB.Output>, NamespaceFilesInterface, InputFilesInterface {

    public static final String DEFAULT_IMAGE = "clickhouse/clickhouse-server:latest";

    private static final ObjectMapper JSON_MAPPER = JacksonMapper.ofJson();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    @Schema(
        title = "The SQL query to execute",
        description = "Single ClickHouse SQL statement. Do not append a FORMAT clause — use `outputFormat` instead."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> query;

    @Schema(
        title = "ClickHouse output format",
        description = """
            Optional ClickHouse output format (see https://clickhouse.com/docs/en/interfaces/formats).

            - Omitted or Pretty* formats (e.g. `PrettyCompact`) → store as Amazon Ion (`.ion`)
            - File formats (e.g. `CSVWithNames`, `Parquet`, `JSONEachRow`) → store with a dedicated extension
            """
    )
    @PluginProperty(group = "main")
    private Property<String> outputFormat;

    @Schema(
        title = "Additional environment variables for the process / container"
    )
    @PluginProperty(dynamic = true, group = "execution")
    protected Map<String, String> env;

    @Schema(
        title = "The task runner to use"
    )
    @Valid
    @PluginProperty(group = "execution")
    @Builder.Default
    private TaskRunner<?> taskRunner = Docker.instance();

    @Schema(
        title = "The container image with clickhouse-local",
        description = "Defaults to the official ClickHouse server image, which includes `clickhouse-local`."
    )
    @PluginProperty(dynamic = true, group = "execution")
    @Builder.Default
    private String containerImage = DEFAULT_IMAGE;

    @PluginProperty(group = "source")
    private NamespaceFiles namespaceFiles;

    @PluginProperty(group = "source")
    private Object inputFiles;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String renderedQuery = runContext.render(this.query).as(String.class).orElseThrow().strip();
        // Strip trailing semicolons; FORMAT / INTO OUTFILE are controlled by this task.
        renderedQuery = renderedQuery.replaceAll(";\\s*$", "");

        if (renderedQuery.isBlank()) {
            throw new IllegalArgumentException("Property 'query' must not be blank");
        }
        // FORMAT as a clause (not format() / formatDateTime()), and INTO OUTFILE, conflict with
        // the task-controlled --format and shell redirection.
        if (renderedQuery.matches("(?is).*\\bFORMAT\\s+[A-Za-z0-9]+.*")) {
            throw new IllegalArgumentException("Query must not include a FORMAT clause; use 'outputFormat' instead");
        }
        if (renderedQuery.matches("(?is).*\\bINTO\\s+OUTFILE\\b.*")) {
            throw new IllegalArgumentException("Query must not include INTO OUTFILE; output files are managed by this task");
        }

        String userFormat = this.outputFormat == null
            ? null
            : runContext.render(this.outputFormat).as(String.class).orElse(null);

        if (userFormat != null) {
            userFormat = userFormat.strip();
            if (userFormat.isEmpty()) {
                userFormat = null;
            } else {
                ChDBOutputFormats.requireSafeFormat(userFormat);
            }
        }

        boolean storeAsIon = ChDBOutputFormats.shouldStoreAsIon(userFormat);
        // Defensive validation: `clickHouseFormat` is interpolated into a shell command.
        String clickHouseFormat = ChDBOutputFormats.requireSafeFormat(
            ChDBOutputFormats.effectiveClickHouseFormat(userFormat)
        );

        // Intermediate file written by clickhouse-local inside the working directory.
        // Ion path uses JSONEachRow then converts; file formats keep their dedicated extension.
        String rawFileName = storeAsIon ? "result.jsonl" : ChDBOutputFormats.outputFileName(userFormat);

        Path workingDir = runContext.workingDir().path();
        Path queryFile = workingDir.resolve("query.sql");
        Files.writeString(queryFile, renderedQuery, StandardCharsets.UTF_8);

        // Prefer shell redirection over INTO OUTFILE so we do not depend on allow_file_output settings.
        // --format=VALUE avoids shell word-splitting for multi-word format names.
        String shellCommand = "clickhouse-local --queries-file=query.sql --format=" + clickHouseFormat
            + " > " + rawFileName;

        ScriptOutput scriptOutput = new CommandsWrapper(runContext)
            .withWarningOnStdErr(true)
            .withTaskRunner(this.taskRunner)
            .withContainerImage(this.containerImage)
            .withEnv(Optional.ofNullable(env).orElse(new HashMap<>()))
            .withNamespaceFiles(namespaceFiles)
            .withInputFiles(inputFiles)
            .withOutputFiles(List.of(rawFileName))
            .withInterpreter(Property.ofValue(List.of("/bin/sh", "-c")))
            .withCommands(Property.ofValue(List.of(shellCommand)))
            .run();

        if (scriptOutput.getExitCode() != 0) {
            throw new IllegalStateException(
                "clickhouse-local exited with code " + scriptOutput.getExitCode()
            );
        }

        Map<String, URI> outputFiles = scriptOutput.getOutputFiles();
        if (outputFiles == null || !outputFiles.containsKey(rawFileName)) {
            throw new IllegalStateException(
                "clickhouse-local did not produce expected output file '" + rawFileName + "'"
            );
        }

        URI rawUri = outputFiles.get(rawFileName);
        Long rowCount = null;
        URI resultUri;

        if (storeAsIon) {
            Path ionPath = runContext.workingDir().createTempFile(ChDBOutputFormats.DEFAULT_ION_EXTENSION);
            rowCount = convertJsonEachRowToIon(runContext, rawUri, ionPath);
            resultUri = runContext.storage().putFile(ionPath.toFile());
        } else {
            // Already uploaded with the correct extension via outputFiles.
            resultUri = rawUri;
        }

        return Output.builder()
            .uri(resultUri)
            .rowCount(rowCount)
            .format(storeAsIon ? "Ion" : clickHouseFormat)
            .build();
    }

    /**
     * Convert JSONEachRow lines from internal storage into an Amazon Ion file.
     *
     * @return number of rows written
     */
    private static long convertJsonEachRowToIon(RunContext runContext, URI jsonEachRowUri, Path ionPath) throws Exception {
        long rows = 0L;
        File ionFile = ionPath.toFile();

        try (
            InputStream inputStream = runContext.storage().getFile(jsonEachRowUri);
            BufferedReader reader = new BufferedReader(
                new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8),
                FileSerde.BUFFER_SIZE
            );
            BufferedOutputStream output = new BufferedOutputStream(
                new FileOutputStream(ionFile),
                FileSerde.BUFFER_SIZE
            )
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                Map<String, Object> row = JSON_MAPPER.readValue(line, MAP_TYPE);
                FileSerde.write(output, row);
                rows++;
            }
        }

        return rows;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URI of the query result in Kestra internal storage",
            description = "Ion file when no format / Pretty* is used; otherwise a file with a format-specific extension."
        )
        private final URI uri;

        @Schema(
            title = "Number of rows written",
            description = "Populated when the result is stored as Ion (default / Pretty* formats). Null for raw file formats."
        )
        private final Long rowCount;

        @Schema(
            title = "Effective storage format label",
            description = "`Ion` for default/Pretty* paths, otherwise the ClickHouse format name used for the artifact."
        )
        private final String format;
    }
}
