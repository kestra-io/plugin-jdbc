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
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URI;
import java.sql.Connection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (Connection connection = this.connection(runContext)) {
            BaseConnection pgConnection = connection.unwrap(BaseConnection.class);
            CopyManager copyManager = new CopyManager(pgConnection);

            String sql = this.query(runContext, runContext.render(this.sql).as(String.class).orElse(null), "TO STDOUT");
            logger.debug("Starting query: {}", sql);

            try (PipedInputStream pipedIn = new PipedInputStream(65536);
                 ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                PipedOutputStream pipedOut = new PipedOutputStream(pipedIn);

                Future<Long> copyFuture = executor.submit(() -> {
                    try {
                        return copyManager.copyOut(sql, pipedOut);
                    } finally {
                        pipedOut.close();
                    }
                });

                URI uri;
                try {
                    uri = runContext.storage().putFile(pipedIn, "copy-out");
                } catch (IOException storageEx) {
                    pipedIn.close();
                    try {
                        copyFuture.get();
                        throw storageEx;
                    } catch (ExecutionException jdbcEx) {
                        Throwable cause = jdbcEx.getCause();
                        throw cause instanceof Exception ex ? ex : jdbcEx;
                    }
                }

                long rowsAffected;
                try {
                    rowsAffected = copyFuture.get();
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    throw cause instanceof Exception ex ? ex : new RuntimeException(cause);
                }

                runContext.metric(Counter.of("rows", rowsAffected));
                return Output
                    .builder()
                    .uri(uri)
                    .rowCount(rowsAffected)
                    .build();
            }
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
