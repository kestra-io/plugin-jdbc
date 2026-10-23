package io.kestra.plugin.jdbc.postgresql;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
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
    title = "Execute a single SQL query against PostgreSQL",
    description = "Runs one SQL statement and fetches results. Supports parameterized queries, transactions with afterSQL, and multiple fetch modes (FETCH, FETCH_ONE, STORE). Default fetchSize is 10,000 rows for STORE mode."
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
                    username: "{{ secret('POSTGRES_USERNAME') }}"
                    password: "{{ secret('POSTGRES_PASSWORD') }}"
                    sql: SELECT concert_id, available, a, b, c, d, play_time, library_record, floatn_test, double_test, real_test, numeric_test, date_type, time_type, timez_type, timestamp_type, timestampz_type, interval_type, pay_by_quarter, schedule, json_type, blob_type FROM pgsql_types
                    fetchType: FETCH

                  - id: use_fetched_data
                    type: io.kestra.plugin.jdbc.postgresql.Queries
                    url: jdbc:postgresql://127.0.0.1:56982/
                    username: "{{ secret('POSTGRES_USERNAME') }}"
                    password: "{{ secret('POSTGRES_PASSWORD') }}"
                    sql:  "{% for row in outputs.fetch.rows %} INSERT INTO pl_store_distribute (year_month,store_code, update_date) values ({{row.play_time}}, {{row.concert_id}}, TO_TIMESTAMP('{{row.timestamp_type}}', 'YYYY-MM-DDTHH:MI:SS.US') ); {% endfor %}"
                """
        ),
        @Example(
            full = true,
            title = "Load a daily table, then read the execution details exposed as outputs: rows affected, duration, bytes, tables touched and optional column statistics.",
            code = """
                id: jdbc_artifacts
                namespace: company.team

                tasks:
                  - id: load_orders
                    type: io.kestra.plugin.jdbc.postgresql.Query
                    url: "{{ secret('PG_URL') }}"
                    username: "{{ secret('PG_USER') }}"
                    password: "{{ secret('PG_PASSWORD') }}"
                    sql: |
                      INSERT INTO analytics.orders_daily (day, total)
                      SELECT date_trunc('day', created_at), sum(amount)
                      FROM public.orders o
                      JOIN public.customers c ON c.id = o.customer_id
                      WHERE o.created_at >= now() - interval '1 day'
                      GROUP BY 1
                    outputStatement: true
                    assets:
                      enableAuto: true

                  - id: sample
                    type: io.kestra.plugin.jdbc.postgresql.Query
                    url: "{{ secret('PG_URL') }}"
                    username: "{{ secret('PG_USER') }}"
                    password: "{{ secret('PG_PASSWORD') }}"
                    sql: SELECT * FROM analytics.orders_daily ORDER BY day DESC LIMIT 1000
                    fetchType: STORE
                    columnStats: true
                    columnStatsSampleRows: 1000

                  - id: report
                    type: io.kestra.plugin.core.log.Log
                    message: |
                      rows affected: {{ outputs.load_orders.rowsAffected }}
                      tables: {{ outputs.load_orders.tables | jq('[.[].name]') }}
                      sample rows: {{ outputs.sample.size }} ({{ outputs.sample.bytes }} bytes, {{ outputs.sample.durationMs }} ms)
                """
        )
    },
    metrics = {
        @Metric(
            name = "fetch.size",
            type = Counter.TYPE,
            unit = "rows",
            description = "The number of fetched rows."
        ),
        @Metric(
            name = "duration",
            type = Timer.TYPE,
            description = "The duration of the SQL execution, including reading the result rows."
        ),
        @Metric(
            name = "rows.affected",
            type = Counter.TYPE,
            unit = "rows",
            description = "The number of rows affected, as reported by the JDBC driver."
        ),
        @Metric(
            name = "bytes",
            type = Counter.TYPE,
            unit = "bytes",
            description = "The size of the stored result file."
        )
    }
)
public class Query extends AbstractJdbcQuery implements PostgresConnectionInterface {
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<Boolean> ssl = Property.ofValue(false);
    @PluginProperty(group = "advanced")
    protected Property<SslMode> sslMode;
    @PluginProperty(group = "advanced")
    protected Property<String> sslRootCert;
    @PluginProperty(group = "advanced")
    protected Property<String> sslCert;
    @PluginProperty(group = "connection")
    protected Property<String> sslKey;
    @PluginProperty(group = "connection", secret = true)
    protected Property<String> sslKeyPassword;

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

    @Override
    protected Integer getFetchSize(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.fetchSize).as(Integer.class).orElse(10000);
    }
}
