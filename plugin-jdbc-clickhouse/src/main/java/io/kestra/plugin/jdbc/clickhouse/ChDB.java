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
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Embedded ClickHouse query task for lightweight transforms.
 *
 * <p>Exposes the chDB-style API ({@code query} + {@code outputFormat}) and runs the query with
 * the {@code clickhouse-local} binary — not the libchdb native library.
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query data with embedded ClickHouse (chDB)",
    description = """
        Runs a single SQL query with `clickhouse-local` for lightweight data transformations — \
        no ClickHouse server required.

        This is the high-level chDB-style API (`query` + optional `outputFormat`). The engine is \
        the `clickhouse-local` binary shipped in the official ClickHouse image, not the libchdb \
        native library.

        When to use which ClickHouse task:
        - `ChDB` — one SQL transform and Kestra-managed storage (Ion by default, or a file format).
        - `ClickHouseLocalCLI` — arbitrary `clickhouse-local` CLI commands, extra flags, or scripts.
        - `Query` / `Queries` — a running ClickHouse server over JDBC.

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
        description = """
            A single ClickHouse SQL statement. A trailing semicolon is ignored.

            Do not append a `FORMAT` clause or `INTO OUTFILE` — use `outputFormat` instead.
            Multiple `;`-separated statements are rejected; use `ClickHouseLocalCLI` for scripts.
            """
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

            Ion path (JSONEachRow → Ion) type notes:
            - ClickHouse defaults `output_format_json_quote_64bit_integers=1`, which would store \
            Int64/UInt64 as JSON strings. This task sets that setting to `0` so 64-bit integers \
            are JSON numbers and stay closer to the JDBC `Query` task.
            - `Date`, `DateTime`, `UUID`, and some other ClickHouse types still serialize as JSON \
            strings, so they may not match JDBC `Query` cell types exactly.
            """
    )
    @PluginProperty(group = "main")
    private Property<String> outputFormat;

    @Schema(
        title = "Additional environment variables for the process / container"
    )
    @PluginProperty(dynamic = true, group = "execution")
    private Map<String, String> env;

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
        String rQuery = normalizeAndValidateQuery(
            runContext.render(this.query).as(String.class).orElseThrow()
        );

        String rOutputFormat = this.outputFormat == null
            ? null
            : runContext.render(this.outputFormat).as(String.class).orElse(null);

        if (rOutputFormat != null) {
            rOutputFormat = rOutputFormat.strip();
            if (rOutputFormat.isEmpty()) {
                rOutputFormat = null;
            } else {
                ChDBOutputFormats.requireSafeFormat(rOutputFormat);
            }
        }

        boolean storeAsIon = ChDBOutputFormats.shouldStoreAsIon(rOutputFormat);
        // Defensive validation: `clickHouseFormat` is interpolated into a shell command.
        String clickHouseFormat = ChDBOutputFormats.requireSafeFormat(
            ChDBOutputFormats.effectiveClickHouseFormat(rOutputFormat)
        );

        // Intermediate file written by clickhouse-local inside the working directory.
        // Ion path uses JSONEachRow then converts; file formats keep their dedicated extension.
        String rawFileName = storeAsIon ? "result.jsonl" : ChDBOutputFormats.outputFileName(rOutputFormat);

        Path workingDir = runContext.workingDir().path();
        Path queryFile = workingDir.resolve("query.sql");
        Files.writeString(queryFile, rQuery, StandardCharsets.UTF_8);

        // Prefer shell redirection over INTO OUTFILE so we do not depend on allow_file_output settings.
        // --format=VALUE avoids shell word-splitting for multi-word format names.
        String shellCommand = clickhouseLocalShellCommand(clickHouseFormat, rawFileName, storeAsIon);

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
                new InputStreamReader(inputStream, StandardCharsets.UTF_8),
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

    /**
     * Strip a trailing semicolon and reject blank, multi-statement, FORMAT, or INTO OUTFILE queries.
     */
    static String normalizeAndValidateQuery(String query) {
        String normalized = query.strip().replaceAll("(?:;\\s*)+$", "");

        if (normalized.isBlank()) {
            throw new IllegalArgumentException("Property 'query' must not be blank");
        }
        if (hasAdditionalStatement(normalized)) {
            throw new IllegalArgumentException(
                "Query must be a single SQL statement; multiple statements are not supported. Use ClickHouseLocalCLI for scripts."
            );
        }
        // Heuristic: FORMAT as a clause (not format() / formatDateTime()) conflicts with the
        // task-controlled --format. This also rejects the word in a string literal or alias,
        // e.g. SELECT 'FORMAT JSON' AS note.
        if (normalized.matches("(?is).*\\bFORMAT\\s+[A-Za-z0-9]+.*")) {
            throw new IllegalArgumentException("Query must not include a FORMAT clause; use 'outputFormat' instead");
        }
        // Same heuristic as FORMAT: INTO OUTFILE in a literal or alias is also rejected.
        if (normalized.matches("(?is).*\\bINTO\\s+OUTFILE\\b.*")) {
            throw new IllegalArgumentException("Query must not include INTO OUTFILE; output files are managed by this task");
        }

        return normalized;
    }

    /**
     * True when {@code sql} still contains a statement-separating semicolon after trailing
     * semicolons were stripped. Semicolons inside quotes or comments are ignored. This is a
     * heuristic, not a full ClickHouse parser.
     */
    static boolean hasAdditionalStatement(String sql) {
        boolean inSingle = false;
        boolean inDouble = false;
        boolean inBacktick = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            char next = i + 1 < sql.length() ? sql.charAt(i + 1) : '\0';

            if (inLineComment) {
                if (c == '\n') {
                    inLineComment = false;
                }
                continue;
            }
            if (inBlockComment) {
                if (c == '*' && next == '/') {
                    inBlockComment = false;
                    i++;
                }
                continue;
            }
            if (inSingle) {
                if (c == '\'') {
                    if (next == '\'') {
                        i++;
                    } else {
                        inSingle = false;
                    }
                }
                continue;
            }
            if (inDouble) {
                if (c == '"') {
                    if (next == '"') {
                        i++;
                    } else {
                        inDouble = false;
                    }
                }
                continue;
            }
            if (inBacktick) {
                if (c == '`') {
                    inBacktick = false;
                }
                continue;
            }

            if (c == '-' && next == '-') {
                inLineComment = true;
                i++;
                continue;
            }
            if (c == '/' && next == '*') {
                inBlockComment = true;
                i++;
                continue;
            }
            if (c == '\'') {
                inSingle = true;
                continue;
            }
            if (c == '"') {
                inDouble = true;
                continue;
            }
            if (c == '`') {
                inBacktick = true;
                continue;
            }
            if (c == ';') {
                return true;
            }
        }
        return false;
    }

    /**
     * Build the clickhouse-local shell command. Ion storage uses JSONEachRow plus JSON type
     * settings so 64-bit integers are numbers rather than quoted strings.
     */
    static String clickhouseLocalShellCommand(String clickHouseFormat, String rawFileName, boolean storeAsIon) {
        StringBuilder command = new StringBuilder("clickhouse-local --queries-file=query.sql --format=")
            .append(clickHouseFormat);
        if (storeAsIon) {
            command.append(" --output_format_json_quote_64bit_integers=0");
        }
        command.append(" > ").append(rawFileName);
        return command.toString();
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
