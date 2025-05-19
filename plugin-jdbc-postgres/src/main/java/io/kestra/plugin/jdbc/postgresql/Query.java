package io.kestra.plugin.jdbc.postgresql;

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
import org.postgresql.Driver;

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
    title = "Query a PostgreSQL server."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Execute a query and fetch results in a task, and update another table with fetched results in a different task.",
            code = """
                id: postgres_query
                namespace: company.team

                tasks:
                  - id: fetch
                    type: io.kestra.plugin.jdbc.postgresql.Query
                    url: jdbc:postgresql://127.0.0.1:56982/
                    username: pg_user
                    password: pg_password
                    sql: select concert_id, available, a, b, c, d, play_time, library_record, floatn_test, double_test, real_test, numeric_test, date_type, time_type, timez_type, timestamp_type, timestampz_type, interval_type, pay_by_quarter, schedule, json_type, blob_type from pgsql_types
                    fetchType: FETCH
                
                  - id: use_fetched_data
                    type: io.kestra.plugin.jdbc.postgresql.Query
                    url: jdbc:postgresql://127.0.0.1:56982/
                    username: pg_user
                    password: pg_password
                    sql:  "{% for row in outputs.fetch.rows %} INSERT INTO pl_store_distribute (year_month,store_code, update_date) values ({{row.play_time}}, {{row.concert_id}}, TO_TIMESTAMP('{{row.timestamp_type}}', 'YYYY-MM-DDTHH:MI:SS.US') ); {% endfor %}"
                """
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, PostgresConnectionInterface, AutoCommitInterface {
    protected final Boolean autoCommit = true;
    @Builder.Default
    protected Boolean ssl = false;
    protected SslMode sslMode;
    protected String sslRootCert;
    protected String sslCert;
    protected String sslKey;
    protected String sslKeyPassword;

    @Override
    public Properties connectionProperties(RunContext runContext) throws Exception {
        Properties properties = super.connectionProperties(runContext);
        PostgresService.handleSsl(properties, runContext, this);

        return properties;
    }

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new PostgresCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(Driver.class::isInstance)) {
            DriverManager.registerDriver(new Driver());
        }
    }

}
