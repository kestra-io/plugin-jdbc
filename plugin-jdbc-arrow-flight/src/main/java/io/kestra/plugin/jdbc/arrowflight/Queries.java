package io.kestra.plugin.jdbc.arrowflight;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
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
    title = "Perform multiple queries on a database through Apache Arrow Flight SQL driver."
)
@Plugin(
    examples = {
        @Example(
            title = "Send SQL queries to a database and fetch row(s) using Apache Arrow Flight SQL driver.",
            full = true,
            code = """
                id: arrow_flight_sql_queries
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.jdbc.arrowflight.Queries
                    url: jdbc:arrow-flight-sql://localhost:31010/?useEncryption=false
                    username: db_user
                    password: db_password
                    sql: SELECT * FROM employee; SELECT * FROM laptop;
                    fetchType: FETCH
                """
        )
    }
)
public class Queries extends AbstractJdbcQueries implements RunnableTask<AbstractJdbcQueries.MultiQueryOutput> {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new ArrowFlightCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new ArrowFlightJdbcDriver());
    }
}
