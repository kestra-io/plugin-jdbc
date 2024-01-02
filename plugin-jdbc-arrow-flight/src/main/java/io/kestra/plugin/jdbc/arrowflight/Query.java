package io.kestra.plugin.jdbc.arrowflight;

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
    title = "Query a database through Apache Arrow Flight SQL driver."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a SQL query to a database and fetch row(s) using Apache Arrow Flight SQL driver.",
            code = {
                "url: jdbc:arrow-flight-sql://localhost:31010/?useEncryption=false",
                "username: db_user",
                "password: db_password",
                "sql: select * FROM departments",
                "fetch: true",
            }
        ),
        @Example(
            title = "Send a sql query to a Dremio coordinator and fetch rows as outputs using Apache Arrow Flight SQL driver.",
            code = {
                "url: jdbc:arrow-flight-sql://dremio-coordinator:32010/?schema=postgres.public",
                "username: dremio_user",
                "password: dremio_password",
                "sql: select * FROM departments",
                "fetch: true",
            }
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output> {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new ArrowFlightCellConverter(zoneId);
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
