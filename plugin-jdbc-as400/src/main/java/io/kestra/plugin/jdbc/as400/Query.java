package io.kestra.plugin.jdbc.as400;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400JDBCDriver;
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
import java.util.Properties;

/**
 * Copied from the DB2 code as we cannot test AS400 we assume it works like DB2
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query an AS400 database."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a SQL query to a AS400 Database and fetch a row as output.",
            full = true,
            code = """
                id: as400_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.jdbc.as400.Query
                    url: jdbc:as400://127.0.0.1:50000/
                    username: "{{ secret('AS400_USERNAME') }}"
                    password: "{{ secret('AS400_PASSWORD') }}"
                    sql: select * from as400_types
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
public class Query extends AbstractJdbcQuery implements As400ConnectionInterface {

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new As400CellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(AS400JDBCDriver.class::isInstance)) {
            DriverManager.registerDriver(new AS400JDBCDriver());
            AS400.setDefaultSignonHandler(new NonInteractiveSignonHandler());
        }
    }

    @Override
    public Properties connectionProperties(RunContext runContext) throws Exception {
        Properties props = super.connectionProperties(runContext, "jdbc:as400");
        props.setProperty("com.ibm.as400.access.AS400.guiAvailable", "false");
        return props;
    }

    @Override
    protected Integer getFetchSize(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.fetchSize).as(Integer.class).orElse(10000);
    }
}
