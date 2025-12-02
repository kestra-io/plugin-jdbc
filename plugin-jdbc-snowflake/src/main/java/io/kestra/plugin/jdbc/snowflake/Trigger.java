package io.kestra.plugin.jdbc.snowflake;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractJdbcTrigger;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import net.snowflake.client.jdbc.SnowflakeDriver;

import java.sql.DriverManager;
import java.sql.SQLException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a flow if a periodically executed Snowflake query returns a non-empty result set."
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for a SQL query to return results, and then iterate through rows.",
            full = true,
            code = """
                id: jdbc_trigger
                namespace: company.team

                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.rows }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ json(taskrun.value) }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.jdbc.snowflake.Trigger
                    interval: "PT5M"
                    url: jdbc:snowflake://<account_identifier>.snowflakecomputing.com
                    username: "{{ secret('SNOWFLAKE_USERNAME') }}"
                    password: "{{ secret('SNOWFLAKE_PASSWORD') }}"
                    sql: "SELECT * FROM demo_db.public.customers"
                    warehouse: COMPUTE_WH
                    fetchType: FETCH
                """
        )
    }
)
public class Trigger extends AbstractJdbcTrigger implements SnowflakeInterface {

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

    @PluginProperty(dynamic = true)
    private Property<String> queryTag;

    @Override
    protected AbstractJdbcQuery.Output runQuery(RunContext runContext) throws Exception {
        var queryBuilder = Query.builder()
            .id(this.id)
            .type(Query.class.getName())
            .url(this.getUrl())
            .timeZoneId(this.getTimeZoneId())
            .sql(this.getSql())
            .afterSQL(this.getAfterSQL())
            .queryTag(this.getQueryTag())
            .fetch(this.isFetch())
            .store(this.isStore())
            .fetchOne(this.isFetchOne())
            .fetchType(Property.ofValue(this.renderFetchType(runContext)))
            .fetchSize(this.getFetchSize())
            .additionalVars(this.additionalVars)
            .warehouse(this.getWarehouse())
            .database(this.getDatabase())
            .parameters(this.getParameters());

        if (this.getUsername() != null) {
            queryBuilder.username(this.getUsername());
        }
        if (this.getPassword() != null) {
            queryBuilder.password(this.getPassword());
        }
        if(this.getPrivateKey() != null) {
            queryBuilder.privateKey(this.getPrivateKey());
        }
        if(this.getPrivateKeyPassword() != null) {
            queryBuilder.privateKeyPassword(this.getPrivateKeyPassword());
        }
        return queryBuilder.build().run(runContext);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(SnowflakeDriver.class::isInstance)) {
            DriverManager.registerDriver(new SnowflakeDriver());
        }
    }
}
