package io.kestra.plugin.jdbc.rockset;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
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

import java.io.IOException;
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
    title = "Query a Rockset server"
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Execute a query and fetch results",
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
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output> {

    protected String apiKey;
    protected String apiServer;

    @Override
    protected Properties connectionProperties(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        Properties properties = super.connectionProperties(runContext);

        properties.put("jdbc.url",  "jdbc:rockset://");
        properties.setProperty("apiKey", apiKey);
        properties.setProperty("apiServer", apiServer);

        return properties;
    }

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new RocksetCellConverter(zoneId);
    }

    @Override
    protected void registerDriver() throws SQLException {
        DriverManager.registerDriver(new com.rockset.jdbc.RocksetDriver());
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        return super.run(runContext);
    }
}
