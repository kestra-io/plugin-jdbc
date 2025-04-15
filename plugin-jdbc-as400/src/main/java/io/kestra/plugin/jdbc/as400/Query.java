package io.kestra.plugin.jdbc.as400;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
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
                    username: as400_user
                    password: as400_password
                    sql: select * from as400_types
                    fetchType: FETCH_ONE
                """
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, As400ConnectionInterface {
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
