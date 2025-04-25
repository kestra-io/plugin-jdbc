package io.kestra.plugin.jdbc.as400;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
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
    title = "Run multiple AS400 database queries."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a SQL query to a AS400 Database and fetch a row as output.",
            full = true,
            code = """
                id: as400_queries
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.jdbc.as400.Queries
                    url: jdbc:as400://127.0.0.1:50000/
                    username: "{{ secret('AS400_USERNAME') }}"
                    password: "{{ secret('AS400_PASSWORD') }}"
                    sql: select * from employee; select * from laptops;
                    fetchType: FETCH
                """
        )
    }
)
public class Queries extends AbstractJdbcQueries implements RunnableTask<AbstractJdbcQueries.MultiQueryOutput>, As400ConnectionInterface {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new As400CellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new com.ibm.as400.access.AS400JDBCDriver());
    }

    @Override
    public Properties connectionProperties(RunContext runContext) throws Exception {
        return super.connectionProperties(runContext, "jdbc:as400");
    }
}
