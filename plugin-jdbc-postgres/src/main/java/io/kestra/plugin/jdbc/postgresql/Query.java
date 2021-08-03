package io.kestra.plugin.jdbc.postgresql;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

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
    title = "PostgresSQL Query Task",
    description = "Currently supported types are the following ones : \n" +
        " - serial\n" +
        " - boolean\n" +
        " - char(4)\n" +
        " - varchar(n)\n" +
        " - text\n" +
        " - smallint\n" +
        " - bigint\n" +
        " - float(n)\n" +
        " - double precision\n" +
        " - real\n" +
        " - numeric\n" +
        " - date\n" +
        " - time\n" +
        " - timez / time with time zone (avoid this type as it can lead to some gap. See <a href=\"https://www.postgresql.org/message-id/4C968069.4050801@opencloud.com/\">this PostgreSQL issue</a>)\n" +
        " - timestamp\n" +
        " - timestampz / timestamp with time zone\n" +
        " - interval\n" +
        " - integer[]\n" +
        " - text[][] (ie. array of array ... it may be of another supported type)\n" +
        " - json\n" +
        " - bytea"
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Execute a query and fetch results on another task to update another table",
            code = {
                "tasks:",
                "- id: update",
                "  type: io.kestra.plugin.jdbc.postgresql.Query",
                "  url: jdbc:postgresql://127.0.0.1:56982/",
                "  username: postgres",
                "  password: pg_passwd",
                "  sql: select concert_id, available, a, b, c, d, play_time, library_record, floatn_test, double_test, real_test, numeric_test, date_type, time_type, timez_type, timestamp_type, timestampz_type, interval_type, pay_by_quarter, schedule, json_type, blob_type from pgsql_types",
                "  fetch: true",
                "- id: use-fetched-data",
                "  type: io.kestra.plugin.jdbc.postgresql.Query",
                "  url: jdbc:postgresql://127.0.0.1:56982/",
                "  username: postgres",
                "  password: pg_passwd",
                "  sql:  \"{{#each outputs.update.rows}} INSERT INTO pl_store_distribute (year_month,store_code, update_date) values ({{this.play_time}}, {{this.concert_id}}, TO_TIMESTAMP('{{this.timestamp_type}}', 'YYYY/MM/DDTHH24:MI:SS.US') ); {{/each}}\""}
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, PostgresConnectionInterface {
    @Builder.Default
    protected Boolean ssl = false;
    protected SslMode sslMode;
    protected String sslRootCert;
    protected String sslCert;
    protected String sslKey;
    protected String sslKeyPassword;

    @Override
    protected Properties connectionProperties(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        Properties properties = super.connectionProperties(runContext);
        PostgresService.handleSsl(properties, runContext, this, this);

        return properties;
    }

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new PostgresCellConverter(zoneId);
    }

    @Override
    protected void registerDriver() throws SQLException {
        DriverManager.registerDriver(new org.postgresql.Driver());
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        return super.run(runContext);
    }
}
