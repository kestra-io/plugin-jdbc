package io.kestra.plugin.jdbc.pinot;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
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
import org.apache.pinot.client.PinotDriver;

import java.sql.*;
import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query an Apache Pinot database."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: pinot_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.jdbc.pinot.Query
                    url: jdbc:pinot://localhost:9000
                    username: "{{ secret('PINOT_USERNAME') }}"
                    password: "{{ secret('PINOT_PASSWORD') }}"
                    sql: |
                      SELECT *
                      FROM airlineStats
                    fetchType: FETCH
                """
        )
    },
    metrics = {
        @Metric(
            name = "fetch.size",
            type = Counter.TYPE,
            unit = "rows",
            description = "The number of fetched rows."
        )
    }
)
public class Query extends AbstractJdbcQuery implements PinotConnectionInterface {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new PinotCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(PinotDriver.class::isInstance)) {
            DriverManager.registerDriver(new PinotDriver());
        }
    }

    @Override
    protected Statement createStatement(Connection conn) throws SQLException {
        return conn.createStatement();
    }

}
