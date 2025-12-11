package io.kestra.plugin.jdbc.pinot;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
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
    title = "Run multiple Apache Pinot queries."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: pinot_queries
                namespace: company.team

                tasks:
                  - id: queries
                    type: io.kestra.plugin.jdbc.pinot.Queries
                    url: jdbc:pinot://localhost:9000
                    username: "{{ secret('PINOT_USERNAME') }}"
                    password: "{{ secret('PINOT_PASSWORD') }}"
                    sql: |
                      SELECT * FROM airlineStats;
                      SELECT * FROM airlineStats;
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
public class Queries extends AbstractJdbcQueries implements PinotConnectionInterface {
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
    protected PreparedStatement createPreparedStatement(Connection conn, String preparedSql) throws SQLException {
        return conn.prepareStatement(preparedSql);
    }

    @Override
    protected Integer getFetchSize(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.fetchSize).as(Integer.class).orElse(Integer.MIN_VALUE);
    }
}
