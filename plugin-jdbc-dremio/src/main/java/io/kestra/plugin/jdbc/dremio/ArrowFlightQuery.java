package io.kestra.plugin.jdbc.dremio;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query a Dremio database through Apache Arrow Flight sql driver."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a sql query to a Dremio direct database and fetch a row as outputs using Apache Arrow Flight sql driver",
            code = {
                "url: jdbc:arrow-flight-sql://localhost:31010/?useEncryption=false",
                "username: dremio",
                "password: dremio123",
                "sql: select * FROM \"postgres.public\".departments",
                "fetch: true",
            }
        ),
        @Example(
            title = "Send a sql query to a Dremio coordinator and fetch a row as outputs using Apache Arrow Flight sql driver",
            code = {
                "url: jdbc:arrow-flight-sql://dremio-coordinator:32010/?schema=postgres.public",
                "username: $token",
                "password: samplePersonalAccessToken",
                "sql: select * FROM departments",
                "fetch: true",
            }
        )
    }
)
public class ArrowFlightQuery extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output> {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new DremioCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new ArrowFlightJdbcDriver());
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        return super.run(runContext);
    }
}
