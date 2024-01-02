package io.kestra.plugin.jdbc.sqlite;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AutoCommitInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
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
    title = "Query a SQLite server."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Execute a query and pass the results to another task.",
            code = {
                "tasks:",
                "- id: update",
                "  type: io.kestra.plugin.jdbc.sqlite.Query",
                "  url: jdbc:sqlite:myfile.db",
                "  sql: select concert_id, available, a, b, c, d, play_time, library_record, floatn_test, double_test, real_test, numeric_test, date_type, time_type, timez_type, timestamp_type, timestampz_type, interval_type, pay_by_quarter, schedule, json_type, blob_type from pgsql_types",
                "  fetch: true",
                "- id: use-fetched-data",
                "  type: io.kestra.plugin.jdbc.sqlite.Query",
                "  url: jdbc:sqlite:myfile.db",
                "  sql: \"{% for row in outputs.update.rows %} INSERT INTO pl_store_distribute (year_month,store_code, update_date) values ({{row.play_time}}, {{row.concert_id}}, TO_TIMESTAMP('{{row.timestamp_type}}', 'YYYY-MM-DDTHH:MI:SS.US') ); {% endfor %}\""}
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, AutoCommitInterface {
    protected final Boolean autoCommit = true;

    @Override
    public Properties connectionProperties(RunContext runContext) throws Exception {
        Properties properties = super.connectionProperties(runContext);

        return properties;
    }

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new SqliteCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new org.sqlite.JDBC());
    }

}
