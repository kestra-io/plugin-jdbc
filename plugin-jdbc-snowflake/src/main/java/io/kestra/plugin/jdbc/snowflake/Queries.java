package io.kestra.plugin.jdbc.snowflake;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import net.snowflake.client.jdbc.SnowflakeDriver;

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
    title = "Run multiple Snowflake queries."
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
                    username: "{{ secret('SNOWFLAKE_USERNAME') }}"
                    password: "{{ secret('SNOWFLAKE_PASSWORD') }}"
                    sql: select * from demo_db.public.customers; select * from demo_db.public.emplyees;
                    fetchType: FETCH
                """
        )
    },
    metrics = {
        @Metric(
            name = "fetch.size",
            type = Counter.TYPE,
            unit = "rows",
            description = "The number of fetched rows."
        )
    }
)
public class Queries extends AbstractJdbcQueries implements SnowflakeInterface {

    @PluginProperty(group = "connection")
    private Property<String> privateKey;

    @PluginProperty(group = "connection")
    private Property<String> privateKeyPassword;

    @PluginProperty(group = "connection")
    private Property<String> database;

    @PluginProperty(group = "connection")
    private Property<String> warehouse;

    @PluginProperty(group = "connection")
    private Property<String> schema;

    @PluginProperty(group = "connection")
    private Property<String> role;

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
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(SnowflakeDriver.class::isInstance)) {
            DriverManager.registerDriver(new SnowflakeDriver());
        }
    }
}
