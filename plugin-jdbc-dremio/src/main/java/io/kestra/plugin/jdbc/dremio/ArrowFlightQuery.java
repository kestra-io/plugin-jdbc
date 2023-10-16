package io.kestra.plugin.jdbc.dremio;

import cfjd.org.apache.arrow.flight.sql.impl.FlightSql;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AutoCommitInterface;
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
    title = "Query a Dremio database with Arrow Flight SQL."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a sql query to a Dremio database and fetch a row as outputs",
            code = {
                "url: jdbc:arrow-flight-sql://data.dremio.cloud:443",
                "username: $token",
                "password: samplePersonalAccessToken",
                "sql: select * FROM source.database.table",
                "fetchOne: true",
            }
        )
    }
)
public class ArrowFlightQuery extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, AutoCommitInterface {
    protected final Boolean autoCommit = true;

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
