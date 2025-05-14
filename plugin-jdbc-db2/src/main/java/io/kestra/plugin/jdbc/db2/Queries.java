package io.kestra.plugin.jdbc.db2;

import com.ibm.db2.jcc.DB2Driver;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AutoCommitInterface;
import io.micronaut.http.uri.UriBuilder;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.net.URI;
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
    title = "Perform multiple queries on a DB2 database."
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
                  - id: queries
                    type: io.kestra.plugin.jdbc.db2.Queries
                    url: jdbc:db2://127.0.0.1:50000/
                    username: db2inst
                    password: db2_password
                    sql: select * from employee; select * from laptop;
                    fetchType: FETCH
                """
        )
    }
)
public class Queries extends AbstractJdbcQueries implements RunnableTask<AbstractJdbcQueries.MultiQueryOutput> {

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new Db2CellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(DB2Driver.class::isInstance)) {
            DriverManager.registerDriver(new DB2Driver());
        }
    }

    @Override
    public Properties connectionProperties(RunContext runContext) throws Exception {
        return super.connectionProperties(runContext, "jdbc:db2");
    }
}
