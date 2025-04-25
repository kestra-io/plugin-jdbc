package io.kestra.plugin.jdbc.db2;

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

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query a DB2 database."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a SQL query to a DB2 Database and fetch a row as output.",
            full = true,
            code = """
                id: db2_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.jdbc.db2.Query
                    url: jdbc:db2://127.0.0.1:50000/
                    username: "{{ secret('DB2_USERNAME') }}"
                    password: "{{ secret('DB2_PASSWORD') }}"
                    sql: select * from db2_types
                    fetchType: FETCH_ONE
                """
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, Db2ConnectionInterface {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new Db2CellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new com.ibm.db2.jcc.DB2Driver());
    }

    @Override
    public Properties connectionProperties(RunContext runContext) throws Exception {
        return super.connectionProperties(runContext, this.getScheme());
    }
}
