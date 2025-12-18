package io.kestra.plugin.jdbc.oracle;

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
import oracle.jdbc.OracleDriver;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run multiple Oracle queries."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Execute multiple queries and get the results.",
            code = """
                id: oracle_query
                namespace: company.team

                tasks:
                  - id: select
                    type: io.kestra.plugin.jdbc.oracle.Queries
                    url: jdbc:oracle:thin:@localhost:49161:XE
                    username: oracle_user
                    password: oracle_password
                    sql: select * from employee; select * from laptop;
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
public class Queries extends AbstractJdbcQueries implements OracleConnectionInterface {

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new OracleCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(OracleDriver.class::isInstance)) {
            DriverManager.registerDriver(new OracleDriver());
        }
    }

    @Override
    protected Integer getFetchSize(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.fetchSize).as(Integer.class).orElse(10000);
    }
}
