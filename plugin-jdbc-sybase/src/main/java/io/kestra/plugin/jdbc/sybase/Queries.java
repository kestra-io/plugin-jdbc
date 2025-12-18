package io.kestra.plugin.jdbc.sybase;

import com.sybase.jdbc4.jdbc.SybDriver;
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

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run multiple Sybase database queries."
)
@Plugin(
    examples = {
        @Example(
            title = "Send SQL queries to a Sybase Database and fetch a row as output.",
            full = true,
            code = """
                   id: sybase_queries
                   namespace: company.team

                   tasks:
                     - id: queries
                       type: io.kestra.plugin.jdbc.sybase.Queries
                       url: jdbc:sybase:Tds:127.0.0.1:5000/
                       username: "{{ secret('SYBASE_USERNAME') }}"
                       password: "{{ secret('SYBASE_PASSWORD') }}"
                       sql: select count(*) from employee, select count(*) from laptop;
                       fetchType: FETCH_ONE
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
public class Queries extends AbstractJdbcQueries implements SybaseConnectionInterface {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new SybaseCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(SybDriver.class::isInstance)) {
            DriverManager.registerDriver(new SybDriver());
        }
    }

    @Override
    protected Integer getFetchSize(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.fetchSize).as(Integer.class).orElse(10000);
    }
}
