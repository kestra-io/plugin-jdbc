package io.kestra.plugin.jdbc.rockset;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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
    title = "Query a Rockset server."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Execute a query and fetch results.",
            code = {
                "tasks:",
                "- id: select",
                "  type: io.kestra.plugin.jdbc.rockset.Query",
                "  url: jdbc:rockset://",
                "  apiKey: \"[apiKey]\"",
                "  apiServer: \"[apiServer]\"",
                "  sql: |",
                "    SELECT *",
                "    FROM nation",
                "  fetch: true"
            }
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, AutoCommitInterface, RocksetConnection {
    protected final Boolean autoCommit = true;

    protected String apiKey;

    protected String apiServer;

    @Override
    public Properties connectionProperties(RunContext runContext) throws Exception {
        Properties properties = super.connectionProperties(runContext);

        properties.setProperty("jdbc.url",  "jdbc:rockset://");
        properties.setProperty("apiKey", runContext.render(apiKey));
        properties.setProperty("apiServer", runContext.render(apiServer));

        return properties;
    }

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new RocksetCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new com.rockset.jdbc.RocksetDriver());
    }

}
