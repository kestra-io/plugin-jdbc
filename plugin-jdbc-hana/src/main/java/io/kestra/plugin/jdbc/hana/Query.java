package io.kestra.plugin.jdbc.hana;

import com.sap.db.jdbc.Driver;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Properties;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;


@SuperBuilder
@Getter
@NoArgsConstructor
@ToString
@EqualsAndHashCode
@Schema(
    title = "Execute a single SQL query against SAP HANA",
    description = "Runs one SQL statement against SAP HANA in-memory database. Optimized for real-time analytics and transactional workloads. Supports parameterized queries, transactions with afterSQL, and multiple fetch modes (FETCH, FETCH_ONE, STORE). Default fetchSize is 10,000 rows."
)
@Plugin(
    examples = {
        @Example(
            title = "Run a SAP HANA query and fetch results",
            full = true,
            code = """
                id: hana_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.jdbc.hana.Query
                    url: jdbc:sap://127.0.0.1:39015/?databaseName=SYSTEMDB
                    username: "{{ secret('HANA_USERNAME') }}"
                    password: "{{ secret('HANA_PASSWORD') }}"
                    sql: SELECT * FROM DUMMY
                    fetchType: FETCH_ONE
                """
        )
    },
    metrics = {
        @Metric(
            name = "fetch.size",
            type = Counter.TYPE,
            unit = "rows",
            description = "Number of fetched rows."
        )
    }
)
public class Query extends AbstractJdbcQuery implements HanaConnectionInterface {

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new HanaCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        if (DriverManager.drivers().noneMatch(Driver.class::isInstance)) {
            DriverManager.registerDriver(new Driver());
        }
    }

    @Override
    public Properties connectionProperties(RunContext runContext) throws Exception {
        return super.connectionProperties(runContext, this.getScheme());
    }
    @Override
    protected Integer getFetchSize(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.fetchSize).as(Integer.class).orElse(Integer.MIN_VALUE);
    }
}
