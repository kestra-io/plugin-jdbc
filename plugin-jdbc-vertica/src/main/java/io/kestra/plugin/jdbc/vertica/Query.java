package io.kestra.plugin.jdbc.vertica;

import com.vertica.jdbc.Driver;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query a Vertica database."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a SQL query to a Vertica database, and fetch a row as output.",
            full = true,
            code = """
                   id: vertica_query
                   namespace: company.team

                   tasks:
                     - id: query
                       type: io.kestra.plugin.jdbc.vertica.Query
                       url: jdbc:vertica://127.0.0.1:56982/db
                       username: "{{ secret('VERTICA_USERNAME') }}"
                       password: "{{ secret('VERTICA_PASSWORD') }}"
                       sql: select * from customer
                       fetchType: FETCH_ONE
                   """
        )
    }
)
public class Query extends AbstractJdbcQuery implements VerticaConnectionInterface {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new VerticaCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(Driver.class::isInstance)) {
            DriverManager.registerDriver(new Driver());
        }
    }
}
