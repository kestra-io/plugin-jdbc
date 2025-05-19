package io.kestra.plugin.jdbc.oracle;

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
import oracle.jdbc.OracleDriver;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query an Oracle database."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Execute a query and fetch results on another task to update another table.",
            code = {
                "tasks:",
                "  - id: select",
                "    type: io.kestra.plugin.jdbc.oracle.Query",
                "    url: jdbc:oracle:thin:@localhost:49161:XE",
                "    username: oracle_user",
                "    password: oracle_passwd",
                "    sql: select * from source",
                "    fetch: true",
                "  - id: generate-update",
                "    type: io.kestra.plugin.jdbc.oracle.Query",
                "    url: jdbc:oracle:thin:@localhost:49161:XE",
                "    username: oracle_user",
                "    password: oracle_passwd",
                "    sql: \"{% for row in outputs.select.rows %} INSERT INTO destination (year_month, store_code, update_date) values ({{ row.year_month }}, {{ row.store_code }}, TO_DATE('{{ row.date }}', 'MONTH DD, YYYY') ); {% endfor %}\""}
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, AutoCommitInterface {
    protected final Boolean autoCommit = true;

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new OracleCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(OracleDriver.class::isInstance)) {
            DriverManager.registerDriver(new OracleDriver());
        }
    }

}
