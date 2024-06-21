package io.kestra.plugin.jdbc.snowflake;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
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
import java.util.Properties;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Wait for a query on a Snowflake database."
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for a SQL query to return results, and then iterate through rows.",
            full = true,
            code = {
                "id: jdbc-trigger",
                "namespace: company.team",
                "",
                "tasks:",
                "  - id: each",
                "    type: io.kestra.plugin.core.flow.EachSequential",
                "    tasks:",
                "      - id: return",
                "        type: io.kestra.plugin.core.debug.Return",
                "        format: \"{{ json(taskrun.value) }}\"",
                "    value: \"{{ trigger.rows }}\"",
                "",
                "triggers:",
                "  - id: watch",
                "    type: io.kestra.plugin.jdbc.snowflake.Trigger",
                "    interval: \"PT5M\"",
                "    url: jdbc:snowflake://<account_identifier>.snowflakecomputing.com",
                "    username: snowflake_user",
                "    password: snowflake_passwd",
                "    sql: \"SELECT * FROM demo_db.public.customers\"",
                "    fetch: true"
            }
        )
    }
)
public class Trigger extends AbstractJdbcTrigger implements SnowflakeInterface {
    private String privateKey;
    private String privateKeyFile;
    private String privateKeyFilePassword;
    private String database;
    private String warehouse;
    private String schema;
    private String role;

    @Override
    protected AbstractJdbcQuery.Output runQuery(RunContext runContext) throws Exception {
        var query = Query.builder()
            .id(this.id)
            .type(Query.class.getName())
            .url(this.getUrl())
            .username(this.getUsername())
            .password(this.getPassword())
            .timeZoneId(this.getTimeZoneId())
            .sql(this.getSql())
            .fetch(this.isFetch())
            .store(this.isStore())
            .fetchOne(this.isFetchOne())
            .fetchSize(this.getFetchSize())
            .additionalVars(this.additionalVars)
            .build();
        return query.run(runContext);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new net.snowflake.client.jdbc.SnowflakeDriver());
    }
}
