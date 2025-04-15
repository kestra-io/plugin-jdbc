package io.kestra.plugin.jdbc.redshift;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query a Redshift database."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a SQL query to a Redshift database and fetch a row as output.",
            full = true,
            code = """
                   id: redshift_query
                   namespace: company.team

                   tasks:
                     - id: select
                       type: io.kestra.plugin.jdbc.redshift.Query
                       url: jdbc:redshift://123456789.eu-central-1.redshift-serverless.amazonaws.com:5439/dev
                       username: admin
                       password: admin_password
                       sql: select * from redshift_types
                       fetchType: FETCH_ONE
                   """
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, RedshiftConnectionInterface {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new RedshiftCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new com.amazon.redshift.jdbc.Driver());
    }

}
