package io.kestra.plugin.jdbc.sqlserver;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
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

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Preform multiple queries against a Microsoft SQL Server."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Execute a query and fetch multiple results.",
            code = """
                id: sqlserver_query
                namespace: company.team

                tasks:
                  - id: select
                    type: io.kestra.plugin.jdbc.sqlserver.Queries
                    url: jdbc:sqlserver://localhost:41433;trustServerCertificate=true
                    username: "{{ secret('SQL_USERNAME') }}"
                    password: "{{ secret('SQL_PASSWORD') }}"
                    sql: select * from employee; select * from laptop;
                    fetchType: FETCH
                """
        )
    }
)
public class Queries extends AbstractJdbcQueries implements RunnableTask<AbstractJdbcQueries.MultiQueryOutput>, SqlServerConnectionInterface {

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new SqlServerCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new com.microsoft.sqlserver.jdbc.SQLServerDriver());
    }
}
