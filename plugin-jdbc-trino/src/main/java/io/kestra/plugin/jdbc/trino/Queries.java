package io.kestra.plugin.jdbc.trino;

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
import io.kestra.plugin.jdbc.ColumnStatsCollector;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.swagger.v3.oas.annotations.media.Schema;
import io.trino.jdbc.TrinoDriver;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute multiple SQL statements using Trino query engine",
    description = "Runs one or more SQL statements separated by semicolons using Trino distributed SQL query engine. Statements run without wrapping them in a transaction by default (`transaction: false`), as Trino catalogs like `memory` accept writes in autocommit mode only; set `transaction: true` to run them in one transaction. The id the coordinator assigned to each query is returned in the `queryId` of the statement outputs and in the `statements` file, and its Trino query stats metrics are emitted. WARNING: Do NOT include a semicolon at the end of your SQL query - it will result in an error. Supports parameterized queries and all fetch modes. Default fetchSize is 10,000 rows."
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
                      CREATE TABLE IF NOT EXISTS memory.default.orders_agg AS SELECT 1 AS id
                      INSERT INTO memory.default.orders_agg VALUES (2)
                      SELECT COUNT(*) AS rows_count FROM memory.default.orders_agg
                    fetchType: FETCH

                  - id: use_results
                    type: io.kestra.plugin.core.debug.Return
                    format: "{{ outputs.transform.outputs[2].rows[0].rows_count }}"
                """
        ),
        @Example(
            full = true,
            title = "Authenticate with an access token and run the statements out of transaction",
            code = """
                id: trino_queries_token
                namespace: company.team

                tasks:
                  - id: ingest
                    type: io.kestra.plugin.jdbc.trino.Queries
                    url: jdbc:trino://trino.example.com:8080/memory
                    accessToken: "{{ secret('TRINO_ACCESS_TOKEN') }}"
                    source: kestra_ingest
                    transaction: false
                    sql: |
                      DELETE FROM memory.default.orders
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

    // The typed connection properties are declared on `TrinoConnectionInterface`; like the Snowflake plugin,
    // they are redeclared as Lombok fields here (and on `Query` and `Trigger`) so their builder, getter and
    // serialization are generated.

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

    @PluginProperty(group = "connection")
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
    @Builder.Default
    private Property<String> source = Property.ofValue("kestra");

    @PluginProperty(group = "advanced")
    private Property<Map<String, Object>> roles;

    @PluginProperty(group = "advanced", secret = true)
    @ToString.Exclude
    private Property<Map<String, Object>> extraCredentials;

    @Getter(AccessLevel.NONE)
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
            this.tags(runContext)
        );
    }

    @Override
    protected Statement createStatement(Connection conn) throws SQLException {
        return attachMonitor(super.createStatement(conn));
    }

    @Override
    protected PreparedStatement createPreparedStatement(Connection conn, String sql) throws SQLException {
        return attachMonitor(super.createPreparedStatement(conn, sql));
    }

    private <T extends Statement> T attachMonitor(T statement) {
        if (this.monitor != null) {
            this.monitor.attach(statement);
        }
        return statement;
    }

    @Override
    protected String afterStatementExecute(RunContext runContext, Statement statement, boolean hasResultSet) {
        if (this.monitor == null) {
            return null;
        }

        if (!hasResultSet) {
            // The driver completed the statement at this point, so the monitored stats are final.
            this.monitor.emitAfterExecute();
        }

        String engineQueryId = this.monitor.engineQueryId();
        this.monitor.logQuery(engineQueryId);

        return engineQueryId;
    }

    @Override
    protected Map<String, Object> fetchResult(ResultSet rs, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
        var result = super.fetchResult(rs, cellConverter, connection);

        if (this.monitor != null) {
            this.monitor.emitFromResultSet(rs);
        }

        return result;
    }

    @Override
    protected long fetch(Statement stmt, ResultSet rs, Consumer<Map<String, Object>> consumer, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
        var count = super.fetch(stmt, rs, consumer, cellConverter, connection);

        if (this.monitor != null) {
            this.monitor.emitFromResultSet(rs);
        }

        return count;
    }

    @Override
    protected long fetchToFile(Statement stmt, ResultSet rs, OutputStream output, AbstractCellConverter cellConverter, Connection connection, ColumnStatsCollector collector) throws SQLException, IOException {
        var count = super.fetchToFile(stmt, rs, output, cellConverter, connection, collector);

        if (this.monitor != null) {
            this.monitor.emitFromResultSet(rs);
        }

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
