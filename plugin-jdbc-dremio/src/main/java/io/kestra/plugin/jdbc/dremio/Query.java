package io.kestra.plugin.jdbc.dremio;

import com.dremio.jdbc.Driver;
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
    title = "Execute a SQL query using Dremio",
    description = "Runs a SQL statement using Dremio data lakehouse platform. Supports querying data lakes, databases, and cloud storage. Supports parameterized queries, transactions with afterSQL, and all fetch modes (FETCH, FETCH_ONE, STORE). Default fetchSize is 10,000 rows."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a SQL query to a Dremio database and fetch a row as output.",
            full = true,
            code = """
                id: dremio_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.jdbc.dremio.Query
                    url: jdbc:dremio:direct=sql.dremio.cloud:443;ssl=true;PROJECT_ID=sampleProjectId;
                    username: dremio_token
                    password: samplePersonalAccessToken
                    sql: SELECT * FROM source.database.table
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
public class Query extends AbstractJdbcQuery implements DremioConnectionInterface {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new DremioCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(Driver.class::isInstance)) {
            DriverManager.registerDriver(new Driver());
        }
    }

    @Override
    protected Integer getFetchSize(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.fetchSize).as(Integer.class).orElse(10000);
    }
}
