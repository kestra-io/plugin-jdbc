package io.kestra.plugin.jdbc.sqlserver;

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
    title = "Query a Microsoft SQL Server database."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Execute a query and fetch results in a task, and update another table with fetched results in a different task.",
            code = """
                id: sqlserver_query
                namespace: company.team

                tasks:
                  - id: select
                    type: io.kestra.plugin.jdbc.sqlserver.Query
                    url: jdbc:sqlserver://localhost:41433;trustServerCertificate=true
                    username: sql_server_user
                    password: sql_server_password
                    sql: select * from source
                    fetchType: FETCH

                  - id: generate_update
                    type: io.kestra.plugin.jdbc.sqlserver.Query
                    url: jdbc:sqlserver://localhost:41433;trustServerCertificate=true
                    username: sql_server_user
                    password: sql_server_password
                    sql: "{% for row in outputs.update.rows %} INSERT INTO destination (year_month, store_code, update_date) values ({{row.year_month}}, {{row.store_code}}, '{{row.date}}'); {% endfor %}"
                """
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, SqlServerConnectionInterface {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new SqlServerCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new com.microsoft.sqlserver.jdbc.SQLServerDriver());
    }

}
