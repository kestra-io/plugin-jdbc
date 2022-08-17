package io.kestra.plugin.jdbc.sqlserver;

import io.kestra.plugin.jdbc.AutoCommitInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
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
    title = "Query a Microsoft SQL Server"
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Execute a query and fetch results on another task to update another table",
            code = {
                "tasks:",
                "- id: select",
                "  type: io.kestra.plugin.jdbc.sqlserver.Query",
                "  url: jdbc:sqlserver://localhost:41433;trustServerCertificate=true",
                "  username: sa",
                "  password: Sqls3rv3r_Pa55word!",
                "  sql: select * from source",
                "  fetch: true",
                "- id: generate-update",
                "  type: io.kestra.plugin.jdbc.sqlserver.Query",
                "  url: jdbc:sqlserver://localhost:41433;trustServerCertificate=true",
                "  username: sa",
                "  password: Sqls3rv3r_Pa55word!",
                "  sql:  \"{% for row in outputs.update.rows %} INSERT INTO destination (year_month, store_code, update_date) values ({{row.year_month}}, {{row.store_code}}, '{{row.date}}'); {% endfor %}\""}
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, AutoCommitInterface {
    protected final Boolean autoCommit = true;

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new SqlServerCellConverter(zoneId);
    }

    @Override
    protected void registerDriver() throws SQLException {
        DriverManager.registerDriver(new com.microsoft.sqlserver.jdbc.SQLServerDriver());
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        return super.run(runContext);
    }
}
