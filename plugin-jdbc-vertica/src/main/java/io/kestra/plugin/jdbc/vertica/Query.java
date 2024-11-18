package io.kestra.plugin.jdbc.vertica;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AutoCommitInterface;
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
    title = "Query a Vertica server."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a SQL query to a Vertica database, and fetch a row as output.",
            full = true,
            code = """
                   id: vertica_query
                   namespace: company.team

                   tasks:
                     - id: query
                       type: io.kestra.plugin.jdbc.vertica.Query
                       url: jdbc:vertica://127.0.0.1:56982/db
                       username: vertica_user
                       password: vertica_password
                       sql: select * from customer
                       fetchType: FETCH_ONE
                   """
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, AutoCommitInterface {
    protected final Boolean autoCommit = true;

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new VerticaCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new com.vertica.jdbc.Driver());
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        return super.run(runContext);
    }
}
