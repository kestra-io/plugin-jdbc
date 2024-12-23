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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.Connection;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Load data into a PostgreSQL table.",
    description = "Copys in CSV, Text, or Binary data into PostgreSQL table."
)
@Plugin(
    examples = {
        @Example(
            title = "Load CSV data into a PostgreSQL table.",
            full = true,
            code = """
                id: postgres_copy_in
                namespace: company.team

                tasks:
                  - id: copy_in
                    type: io.kestra.plugin.jdbc.postgresql.CopyIn
                    url: jdbc:postgresql://127.0.0.1:56982/
                    username: pg_user
                    password: pg_password
                    format: CSV
                    from: "{{ outputs.export.uri }}"
                    table: my_destination_table
                    header: true
                    delimiter: "\\t"
                """
        )
    }
)
public class CopyIn extends AbstractCopy implements RunnableTask<CopyIn.Output>, PostgresConnectionInterface {
    @NotNull
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Source file URI."
    )
    private Property<String> from;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        try (
            Connection connection = this.connection(runContext);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)));
        ) {
            BaseConnection pgConnection = connection.unwrap(BaseConnection.class);
            CopyManager copyManager = new CopyManager(pgConnection);

            String sql = this.query(runContext, null, "FROM STDIN");

            logger.debug("Starting query: {}", sql);

            long rowsAffected = copyManager.copyIn(sql, bufferedReader);
            runContext.metric(Counter.of("rows", rowsAffected));

            return Output
                .builder()
                .rowCount(rowsAffected)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The rows count from this `COPY`."
        )
        private final Long rowCount;
    }
}
