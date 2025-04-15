package io.kestra.plugin.jdbc.snowflake;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
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
                    username: snowflake_user
                    password: snowflake_password
                    sql: "SELECT * FROM demo_db.public.customers"
                    warehouse: COMPUTE_WH
                    fetchType: FETCH
                """
        )
    }
)
public class Trigger extends AbstractJdbcTrigger implements SnowflakeInterface {
    private Property<String> privateKey;
    private Property<String> privateKeyPassword;
    private Property<String> database;
    private Property<String> warehouse;
    private Property<String> schema;
    private Property<String> role;

    @Override
    protected AbstractJdbcQuery.Output runQuery(RunContext runContext) throws Exception {
        var queryBuilder = Query.builder()
            .id(this.id)
            .type(Query.class.getName())
            .url(this.getUrl())
            .timeZoneId(this.getTimeZoneId())
            .sql(this.getSql())
            .fetch(this.isFetch())
            .store(this.isStore())
            .fetchOne(this.isFetchOne())
            .fetchType(Property.of(this.renderFetchType(runContext)))
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
        DriverManager.registerDriver(new net.snowflake.client.jdbc.SnowflakeDriver());
    }
}
