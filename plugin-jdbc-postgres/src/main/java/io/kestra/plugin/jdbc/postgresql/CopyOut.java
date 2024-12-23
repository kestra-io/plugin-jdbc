package io.kestra.plugin.jdbc.postgresql;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.net.URI;
import java.nio.file.Path;
import java.sql.Connection;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Copy tabular data from a PostgreSQL table to a file."
)
@Plugin(
    examples = {
        @Example(
            title = "Export a PostgreSQL table or query to a CSV or TSV file.",
            code = """
                id: postgres_copy_out
                namespace: company.team

                tasks:
                  - id: copy_out
                    type: io.kestra.plugin.jdbc.postgresql.CopyOut
                    url: jdbc:postgresql://127.0.0.1:56982/
                    username: pg_user
                    password: pg_password
                    format: CSV
                    sql: SELECT 1 AS int, 't'::bool AS bool UNION SELECT 2 AS int, 'f'::bool AS bool
                    header: true
                    delimiter: "\\t"
                """
        )
    }
)
public class CopyOut extends AbstractCopy implements RunnableTask<CopyOut.Output>, PostgresConnectionInterface {
    @Schema(
        title = "A SELECT, VALUES, INSERT, UPDATE or DELETE command whose results are to be copied.",
        description = "For INSERT, UPDATE and DELETE queries a RETURNING clause must be provided, and the target relation must not have a conditional rule, nor an ALSO rule, nor an INSTEAD rule that expands to multiple statements."
    )
    protected Property<String> sql;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        Path path = runContext.workingDir().createTempFile();

        try (
            Connection connection = this.connection(runContext);
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(path.toFile()));
        ) {
            BaseConnection pgConnection = connection.unwrap(BaseConnection.class);
            CopyManager copyManager = new CopyManager(pgConnection);

            String sql = this.query(runContext, runContext.render(this.sql).as(String.class).orElse(null), "TO STDOUT");

            logger.debug("Starting query: {}", sql);

            long rowsAffected  = copyManager.copyOut(sql, bufferedWriter);
            runContext.metric(Counter.of("rows", rowsAffected));

            bufferedWriter.flush();

            return Output
                .builder()
                .uri(runContext.storage().putFile(path.toFile()))
                .rowCount(rowsAffected)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The URI of the result file on Kestra's internal storage."
        )
        private final URI uri;

        @Schema(
            title = "The rows count from this `COPY`."
        )
        private final Long rowCount;
    }
}
