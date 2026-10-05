package io.kestra.plugin.jdbc.postgresql;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URI;
import java.sql.Connection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.UnaryOperator;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.enums.MonacoLanguages;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Copy tabular data from a PostgreSQL table to a file",
    description = "Exports tabular data from a PostgreSQL table or query to a file using the COPY command."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Export a PostgreSQL table or query to a CSV or TSV file.",
            code = """
                id: postgres_copy_out
                namespace: company.team

                tasks:
                  - id: copy_out
                    type: io.kestra.plugin.jdbc.postgresql.CopyOut
                    url: jdbc:postgresql://sample_postgres:5432/world
                    username: "{{ secret('POSTGRES_USERNAME') }}"
                    password: "{{ secret('POSTGRES_PASSWORD') }}"
                    format: CSV
                    sql: SELECT 1 AS int, 't'::bool AS bool UNION SELECT 2 AS int, 'f'::bool AS bool
                    header: true
                    delimiter: "\\t"
                """
        ),
        @Example(
            full = true,
            title = "Export output of a Postgres SQL query to a CSV file",
            code = """
                id: export_from_postgres
                namespace: company.team

                tasks:
                  - id: export
                    type: io.kestra.plugin.jdbc.postgresql.CopyOut
                    url: jdbc:postgresql://sample_postgres:5432/world
                    username: "{{ secret('POSTGRES_USERNAME') }}"
                    password: "{{ secret('POSTGRES_PASSWORD') }}"
                    format: CSV
                    header: true
                    sql: SELECT * FROM country LIMIT 10
                    delimiter: ","

                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ outputs.export.rowCount }}"
                """
            ),
        @Example(
            full = true,
            title = "Export JSON payloads from a text column without COPY TEXT escaping",
            code = """
                id: export_json_from_postgres
                namespace: company.team
                tasks:
                  - id: export
                    type: io.kestra.plugin.jdbc.postgresql.CopyOut
                    url: jdbc:postgresql://sample_postgres:5432/world
                    username: "{{ secret('POSTGRES_USERNAME') }}"
                    password: "{{ secret('POSTGRES_PASSWORD') }}"
                    format: TEXT
                    outputMode: RAW
                    sql: SELECT payload FROM events
                """
        )
    },
    metrics = {
        @Metric(
            name = "rows",
            type = Counter.TYPE,
            unit = "rows",
            description = "The number of rows copied from PostgreSQL."
        )
    }
)

public class CopyOut extends AbstractCopy implements RunnableTask<CopyOut.Output>, PostgresConnectionInterface {

    @Schema(
        title = "A SELECT, VALUES, INSERT, UPDATE or DELETE command whose results are to be copied",
        description = "For INSERT, UPDATE and DELETE queries a RETURNING clause must be provided, and the target relation must not have a conditional rule, nor an ALSO rule, nor an INSTEAD rule that expands to multiple statements."
    )
    @PluginProperty(language = MonacoLanguages.SQL, group = "main")
    protected Property<String> sql;

    @Schema(
        title = "Output mode",
        description = "Controls how the output of the COPY TEXT command is handled. The default value, `COPY`, preserves PostgreSQL's COPY TEXT representation, including its escaping of backslashes, delimiters, and line breaks. `RAW` removes the COPY TEXT escaping, which is useful when exporting a single column of JSON or other serialized text. Because escaping is removed, delimiters and line breaks inside values can no longer be told apart from column and row separators, and the default NULL marker `\\N` becomes `N`; set `nullString` if NULLs must stay distinguishable. This option is allowed only when using TEXT format."
    )
    @PluginProperty(group = "processing")
    @Builder.Default
    protected Property<OutputMode> outputMode = Property.ofValue(OutputMode.COPY);

    public enum OutputMode {
        COPY,
        RAW
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        Format format = runContext.render(this.format)
                .as(Format.class)
                .orElseThrow(() -> new IllegalArgumentException("format is required"));

        OutputMode outputMode = runContext.render(this.outputMode)
                .as(OutputMode.class)
                .orElse(OutputMode.COPY);

        if (outputMode == OutputMode.RAW && format != Format.TEXT) {
            throw new IllegalArgumentException("RAW output mode is only allowed with TEXT format");
        }

        try (Connection connection = this.connection(runContext)) {
            BaseConnection pgConnection = connection.unwrap(BaseConnection.class);
            CopyManager copyManager = new CopyManager(pgConnection);

            String sql = this.query(runContext, runContext.render(this.sql).as(String.class).orElse(null), "TO STDOUT");
            logger.debug("Starting query: {}", sql);

            if (outputMode == OutputMode.RAW) {
                return this.storeCopyOutput(
                    runContext,
                    copyManager,
                    sql,
                    CopyTextDecoderInputStream::new
                );
            }

            return this.storeCopyOutput(
                runContext,
                copyManager,
                sql,
                UnaryOperator.identity());
        }
    }

    private Output storeCopyOutput(
            RunContext runContext,
            CopyManager copyManager,
            String sql,
            UnaryOperator<InputStream> inputWrapper) throws Exception {
        try (PipedInputStream input = new PipedInputStream(65536);
                PipedOutputStream output = new PipedOutputStream(input);
                ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {

            Future<Long> copyFuture = executor.submit(() -> {
                try {
                    return copyManager.copyOut(sql, output);
                } finally {
                    output.close();
                }
            });

            URI uri;
            try (InputStream storageInput = inputWrapper.apply(input)) {
                uri = runContext.storage().putFile(storageInput, "copy-out");
            } catch (IOException storageEx) {
                input.close();
                awaitCopy(copyFuture);
                throw storageEx;
            }

            return buildOutput(runContext, uri, awaitCopy(copyFuture));
        }
    }

    private long awaitCopy(Future<Long> copyFuture) throws Exception {
        try {
            return copyFuture.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw cause instanceof Exception ex ? ex : new RuntimeException(cause);
        }
    }

    private Output buildOutput(RunContext runContext, URI uri, long rowsAffected) {
        runContext.metric(Counter.of("rows", rowsAffected));
        return Output.builder()
                .uri(uri)
                .rowCount(rowsAffected)
                .build();
    }

    private static final class CopyTextDecoderInputStream extends InputStream {
        private final InputStream source;
        private final byte[] buffer = new byte[8192];
        private int position;
        private int limit;
        private boolean escaped;
        private boolean isEndOfStream;

        private CopyTextDecoderInputStream(InputStream source) {
            this.source = source;
        }

        @Override
        public int read() throws IOException {
            int value = readSource();
            isEndOfStream = value < 0;

            if (isEndOfStream) {
                boolean hasPendingEscape = escaped;
                escaped = false;

                if (hasPendingEscape) {
                    return '\\';
                }
                return -1;
            }

            if (!escaped && value == '\\')
            {
                escaped = true;
                return read();
            }

            if (escaped) {
                escaped = false;
                return decodedEscapeByte(value);
            }

            return value;
        }

        private int decodedEscapeByte(int value) {
            return switch (value) {
                case 'b' -> '\b';
                case 'f' -> '\f';
                case 'n' -> '\n';
                case 'r' -> '\r';
                case 't' -> '\t';
                case 'v' -> '\u000B';
                case '\\' -> '\\';
                default -> value;
            };
        }

        private int readSource() throws IOException {
            if (position >= limit) {
                limit = source.read(buffer);
                position = 0;
                if (limit < 0) {
                    return -1;
                }
            }
            return Byte.toUnsignedInt(buffer[position++]);
        }

        @Override
        public void close() throws IOException {
            source.close();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The URI of the result file on Kestra's internal storage"
        )
        private final URI uri;

        @Schema(
            title = "The rows count from this `COPY`"
        )
        private final Long rowCount;
    }
}
