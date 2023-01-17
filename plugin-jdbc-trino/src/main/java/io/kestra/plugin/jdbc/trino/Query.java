package io.kestra.plugin.jdbc.trino;

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
    title = "Query a Trino server"
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Execute a query and fetch results on another task to update another table",
            code = {
                "tasks:",
                "- id: select",
                "  type: io.kestra.plugin.jdbc.trino.Query",
                "  url: jdbc:trino://localhost:8080/tpch/sf1",
                "  sql: |",
                "    SELECT *",
                "    FROM nation",
                "  fetch: true",
                "- id: generate-update",
                "  type: io.kestra.plugin.jdbc.trino.Query",
                "  url: jdbc:trino://localhost:8080/memory/default",
                "  sql:  \"{% for row in outputs.update.rows %} INSERT INTO destination (nationkey, name, regionkey, comment) values ({{row.nationkey}}, {{row.name}}, '{{row.regionkey}}', '{{row.comment}}'); {% endfor %}\""}
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, AutoCommitInterface {
    protected final Boolean autoCommit = true;

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new TrinoCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new io.trino.jdbc.TrinoDriver());
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        return super.run(runContext);
    }
}
