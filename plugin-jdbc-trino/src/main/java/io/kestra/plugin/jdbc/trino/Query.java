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
    title = "Query data using Trino Query Engine. Make sure NOT to include semicolon at the end of your SQL query. Adding semicolon at the end will result in an error. If you want to test this integration, search for Trino in Blueprints - you'll find detailed instructions there."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Execute a query and fetch results to pass it to downstream tasks.",
            code = {
                    "tasks:",
                    "  - id: analyzeOrders",
                    "    type: io.kestra.plugin.jdbc.trino.Query",
                    "    url: jdbc:trino://localhost:8080/tpch",
                    "    username: trino_user",
                    "    password: trino_passwd",
                    "    sql: |",
                    "      select orderpriority as priority, sum(totalprice) as total",
                    "      from tpch.tiny.orders",
                    "      group by orderpriority",
                    "      order by orderpriority",
                    "    fetch: true",
                    "    store: true",
                    "  - id: csvReport",
                    "    type: io.kestra.plugin.serdes.csv.IonToCsv",
                    "    from: \"{{ outputs.analyzeOrders.uri }}\""}
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

}
