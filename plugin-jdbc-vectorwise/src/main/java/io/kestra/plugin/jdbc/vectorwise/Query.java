package io.kestra.plugin.jdbc.vectorwise;

import com.ingres.jdbc.IngresDriver;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.runners.RunContext;
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
    title = "Execute a single SQL query against Vectorwise",
    description = "Runs one SQL statement against Actian Vector (formerly Vectorwise) columnar analytics database. Optimized for analytical workloads and high-performance queries. Supports parameterized queries, transactions with afterSQL, and multiple fetch modes (FETCH, FETCH_ONE, STORE). Default fetchSize is 10,000 rows."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a SQL query to a Vectorwise database and fetch a row as output.",
            full = true,
            code = """
                   id: vectorwise_query
                   namespace: company.team

                   tasks:
                     - id: query
                       type: io.kestra.plugin.jdbc.vectorwise.Query
                       url: jdbc:vectorwise://url:port/base
                       username: "{{ secret('VECTORWISE_USERNAME') }}"
                       password: "{{ secret('VECTORWISE_PASSWORD') }}"
                       sql: select * from vectorwise_types
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
public class Query extends AbstractJdbcQuery implements VetorwiseConnectionInterface {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new VectorwiseCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(IngresDriver.class::isInstance)) {
            DriverManager.registerDriver(new IngresDriver());
        }
    }

    @Override
    protected Integer getFetchSize(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.fetchSize).as(Integer.class).orElse(10000);
    }
}
