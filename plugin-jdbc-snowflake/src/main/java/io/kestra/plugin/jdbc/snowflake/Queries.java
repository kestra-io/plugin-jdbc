package io.kestra.plugin.jdbc.snowflake;

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

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Perform multiple queries on a Snowflake server."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Execute queries and fetch results in a task, and update another table with fetched results in a different task.",
            code = """
                id: snowflake_queries
                namespace: company.team
                
                tasks:
                  - id: select
                    type: io.kestra.plugin.jdbc.snowflake.Queries
                    url: jdbc:snowflake://<account_identifier>.snowflakecomputing.com
                    username: snowflake_user
                    password: snowflake_password
                    sql: select * from demo_db.public.customers; select * from demo_db.public.emplyees;
                    fetchType: FETCH
                """
        )
    }
)
public class Queries extends AbstractJdbcQueries implements RunnableTask<AbstractJdbcQueries.MultiQueryOutput>, SnowflakeInterface {
    private String privateKey;
    private String privateKeyFile;
    private String privateKeyFilePassword;
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
