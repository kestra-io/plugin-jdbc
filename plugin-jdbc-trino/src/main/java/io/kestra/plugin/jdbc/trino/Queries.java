package io.kestra.plugin.jdbc.trino;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

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
import io.kestra.plugin.jdbc.AbstractJdbcQueries;

import io.swagger.v3.oas.annotations.media.Schema;
import io.trino.jdbc.TrinoDriver;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute multiple SQL statements using Trino query engine",
    description = """
        Runs one or more SQL statements separated by semicolons using Trino distributed SQL query engine.
        Statements run without wrapping them in a transaction by default (`transaction: false`), as Trino catalogs like `memory` accept writes in autocommit mode only.
        Set `transaction: true` to run them in one transaction.
        The id the coordinator assigned to each query is returned in the `queryId` of the statement outputs and in the `statements` file, and its Trino query stats metrics are emitted.
        With fetchType NONE, result sets are not drained just to collect stats, so emitted stats may be incomplete.
        Supports parameterized queries and all fetch modes. Default fetchSize is 10,000 rows."""
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Create a table, insert rows and query them in one task",
            code = """
                id: trino_queries
                namespace: company.team

                tasks:
                  - id: transform
                    type: io.kestra.plugin.jdbc.trino.Queries
                    url: jdbc:trino://localhost:8080/memory
                    username: "{{ secret('TRINO_USERNAME') }}"
                    sql: |
                      CREATE TABLE IF NOT EXISTS memory.default.orders_agg AS SELECT 1 AS id;
                      INSERT INTO memory.default.orders_agg VALUES (2);
                      SELECT COUNT(*) AS rows_count FROM memory.default.orders_agg
                    fetchType: FETCH

                  - id: log_results
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ outputs.transform.outputs[0].rows[0].rows_count }}"
                """
        ),
        @Example(
            full = true,
            title = "Authenticate with an access token and run the statements in autocommit mode",
            code = """
                id: trino_queries_token
                namespace: company.team

                tasks:
                  - id: ingest
                    type: io.kestra.plugin.jdbc.trino.Queries
                    url: jdbc:trino://trino.example.com:443/memory
                    ssl: true
                    accessToken: "{{ secret('TRINO_ACCESS_TOKEN') }}"
                    source: kestra_ingest
                    sql: |
                      DELETE FROM memory.default.orders;
                      INSERT INTO memory.default.orders SELECT * FROM tpch.tiny.orders
                    fetchType: NONE
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
        ),
        @Metric(
            name = "processed.rows",
            type = Counter.TYPE,
            unit = "rows",
            description = "The number of rows processed by the Trino coordinator for the queries."
        ),
        @Metric(
            name = "processed.bytes",
            type = Counter.TYPE,
            unit = "bytes",
            description = "The number of bytes processed by the Trino coordinator for the queries."
        ),
        @Metric(
            name = "peak.memory.bytes",
            type = Counter.TYPE,
            unit = "bytes",
            description = "The peak memory bytes used by the Trino coordinator for the queries."
        ),
        @Metric(
            name = "physical.written.bytes",
            type = Counter.TYPE,
            unit = "bytes",
            description = "The number of bytes physically written by the Trino coordinator for the queries."
        ),
        @Metric(
            name = "cpu.time",
            type = Timer.TYPE,
            description = "The total CPU time spent by the Trino coordinator for the queries."
        ),
        @Metric(
            name = "wall.time",
            type = Timer.TYPE,
            description = "The wall clock time spent by the Trino coordinator for the queries."
        ),
        @Metric(
            name = "queued.time",
            type = Timer.TYPE,
            description = "The time the queries spent queued on the Trino coordinator before running."
        )
    }
)
public class Queries extends AbstractJdbcQueries implements TrinoConnectionInterface {

    @PluginProperty(group = "connection", secret = true)
    @ToString.Exclude
    private Property<String> accessToken;

    @PluginProperty(group = "connection")
    private Property<String> catalog;

    @PluginProperty(group = "connection")
    private Property<String> schema;

    @PluginProperty(group = "connection")
    private Property<Boolean> ssl;

    @PluginProperty(group = "connection")
    private Property<SslVerification> sslVerification;

    @PluginProperty(group = "connection", secret = true)
    @ToString.Exclude
    private Property<String> sslKeyStore;

    @PluginProperty(group = "connection", secret = true)
    @ToString.Exclude
    private Property<String> sslKeyStorePassword;

    @PluginProperty(group = "connection")
    private Property<String> sslKeyStoreType;

    @PluginProperty(group = "connection")
    private Property<String> sslTrustStore;

    @PluginProperty(group = "connection", secret = true)
    @ToString.Exclude
    private Property<String> sslTrustStorePassword;

    @PluginProperty(group = "connection")
    private Property<String> sslTrustStoreType;

    @PluginProperty(group = "connection")
    private Property<String> kerberosPrincipal;

    @PluginProperty(group = "connection")
    private Property<String> kerberosRemoteServiceName;

    @PluginProperty(group = "connection", secret = true)
    @ToString.Exclude
    private Property<String> kerberosKeytab;

    @PluginProperty(group = "connection")
    private Property<String> kerberosConfig;

    @PluginProperty(group = "advanced")
    private Property<Map<String, Object>> sessionProperties;

    @PluginProperty(group = "advanced")
    private Property<List<String>> clientTags;

    @PluginProperty(group = "advanced")
    private Property<String> source;

    @PluginProperty(group = "advanced")
    private Property<Map<String, Object>> roles;

    @PluginProperty(group = "advanced", secret = true)
    @ToString.Exclude
    private Property<Map<String, Object>> extraCredentials;

    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private transient TrinoQueryMonitor monitor;

    @Override
    protected boolean transactionalByDefault() {
        return false;
    }

    @Override
    protected void beforeExecute(RunContext runContext, Connection connection) throws IllegalVariableEvaluationException {
        this.monitor = new TrinoQueryMonitor(
            runContext,
            runContext.render(this.getUrl()).as(String.class).orElse(null),
            runContext.render(this.getSsl()).as(Boolean.class).orElse(null),
            this.tags(runContext)
        );
    }

    @Override
    protected PreparedStatement createPreparedStatement(Connection conn, String sql) throws SQLException {
        return monitor.attach(super.createPreparedStatement(conn, sql));
    }

    @Override
    protected String afterStatementExecute(RunContext runContext, Statement statement, boolean hasResultSet) {
        return monitor.afterExecute(hasResultSet);
    }

    @Override
    protected void discardResultSet(ResultSet resultSet) throws SQLException {
        monitor.discardResults();
    }

    @Override
    protected Map<String, Object> fetchResult(ResultSet rs, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
        var result = super.fetchResult(rs, cellConverter, connection);

        this.monitor.emitFromResultSet(rs);

        return result;
    }

    @Override
    protected long fetch(Statement stmt, ResultSet rs, Consumer<Map<String, Object>> consumer, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
        var count = super.fetch(stmt, rs, consumer, cellConverter, connection);

        this.monitor.emitFromResultSet(rs);

        return count;
    }

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new TrinoCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(TrinoDriver.class::isInstance)) {
            DriverManager.registerDriver(new TrinoDriver());
        }
    }

    @Override
    protected Integer getFetchSize(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.fetchSize).as(Integer.class).orElse(10000);
    }
}
