package io.kestra.plugin.jdbc.oracle;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute a batch query to a Oracle server"
)
@Plugin(
    examples = {
        @Example(
            title = "Fetch rows from a table and bulk insert to another one",
            full = true,
            code = {
                "tasks:",
                "  - id: query",
                "    type: io.kestra.plugin.jdbc.oracle.Query",
                "    url: jdbc:oracle:thin:@dev:49161:XE",
                "    username: oracle",
                "    password: oracle_passwd",
                "    sql: |",
                "      SELECT *",
                "      FROM xref",
                "      LIMIT 1500;",
                "    store: true",
                "  - id: update",
                "    type: io.kestra.plugin.jdbc.oracle.Batch",
                "    from: \"{{ outputs.query.uri }}\"",
                "    url: jdbc:oracle:thin:@prod:49161:XE",
                "    username: oracle",
                "    password: oracle_passwd",
                "    sql: |",
                "      insert into xref values( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )",
            }
        )
    }
)
public class Batch extends AbstractJdbcBatch implements RunnableTask<AbstractJdbcBatch.Output> {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new OracleCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
    }
}
