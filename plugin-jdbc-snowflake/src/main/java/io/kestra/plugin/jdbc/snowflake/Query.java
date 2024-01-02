package io.kestra.plugin.jdbc.snowflake;

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
import java.util.Properties;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query a Snowflake server."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Execute a query and fetch results in a task, and update another table with fetched results in a different task.",
            code = {
                "tasks:",
                "- id: select",
                "  type: io.kestra.plugin.jdbc.snowflake.Query",
                "  url: jdbc:snowflake://<account_identifier>.snowflakecomputing.com",
                "  username: snowflake_user",
                "  password: snowflake_passwd",
                "  sql: select * from source",
                "  fetch: true",
                "- id: generate-update",
                "  type: io.kestra.plugin.jdbc.snowflake.Query",
                "  url: jdbc:snowflake://<account_identifier>.snowflakecomputing.com",
                "  username: snowflake_user",
                "  password: snowflake_passwd",
                "  sql:  \"{% for row in outputs.update.rows %} INSERT INTO destination (year_month, store_code, update_date) values ({{row.year_month}}, {{row.store_code}}, TO_DATE('{{row.date}}', 'MONTH DD, YYYY') ); {% endfor %}\""}
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, SnowflakeInterface, AutoCommitInterface {
    protected final Boolean autoCommit = true;

    private String database;
    private String warehouse;
    private String schema;
    private String role;

    @Override
    public Properties connectionProperties(RunContext runContext) throws Exception {
        Properties properties = super.connectionProperties(runContext);

        this.renderProperties(runContext, properties);

        return properties;
    }

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new SnowflakeCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new net.snowflake.client.jdbc.SnowflakeDriver());
    }
}
