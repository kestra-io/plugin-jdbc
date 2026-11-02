package io.kestra.plugin.jdbc.trino;

import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.runners.RunContext;
import io.trino.jdbc.QueryStats;
import io.trino.jdbc.TrinoResultSet;
import io.trino.jdbc.TrinoStatement;

import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

/**
 * Follows the Trino query of the statements the surrounding task executes, exposes the engine query id
 * and translates the final driver query stats into task metrics.
 *
 * <p>
 * The stats collector is attached to the driver statement before execution (from the task
 * {@code createStatement}/{@code createPreparedStatement} overrides), so the driver reports status
 * updates while the query runs. Results read by the plugin emit from the result set itself via
 * {@link #emitFromResultSet(ResultSet)}, statements with no result set (and SELECTs whose rows were
 * never read) emit from {@link #emitAfterExecute()} when the driver already completed the query.
 * Either way every statement emits its stats once.
 */
public final class TrinoQueryMonitor {

    private final RunContext runContext;
    private final String[] metricTags;
    private final String webUiBase;
    private final TrinoQueryStatsCollector collector = new TrinoQueryStatsCollector();

    // Stats are emitted from the task run only (the collector is a passive sink), so no synchronization is needed.
    private boolean emitted;

    public TrinoQueryMonitor(RunContext runContext, String jdbcUrl, Boolean ssl, String... metricTags) {
        this.runContext = runContext;
        this.metricTags = metricTags;
        this.webUiBase = queryUiBase(jdbcUrl, ssl);
    }

    /** Attaches the stats collector to the driver statement and prepares the monitoring of its query. */
    public <T extends Statement> T attach(T statement) {
        collector.reset();
        emitted = false;

        if (statement instanceof TrinoStatement trinoStatement) {
            trinoStatement.setProgressMonitor(collector);
        }
        return statement;
    }

    public String afterExecute(boolean hasResultSet) {
        if (!hasResultSet) {
            emitAfterExecute();
        }
        String queryId = engineQueryId();
        logQuery(queryId);
        return queryId;
    }

    public void discardResults(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
            // Drain the result so the driver receives final statistics.
        }
        emitFromResultSet(resultSet);
    }

    /** The Trino engine query id of the monitored statement, or `null` while no status was received. */
    public String engineQueryId() {
        return collector.queryId();
    }

    /** Logs the engine query id with the link to its Trino Web UI page. */
    public void logQuery(String queryId) {
        if (queryId == null) {
            return;
        }
        var webUiUrl = webUiBase != null ? webUiBase + "query.html?" + queryId : null;
        if (webUiUrl != null) {
            runContext.logger().info("Trino query executed with id `{}`, details in the Trino Web UI at `{}`", queryId, webUiUrl);
        } else {
            runContext.logger().info("Trino query executed with id `{}`", queryId);
        }
    }

    /**
     * Emits the query stats of a statement that completed during `Statement.execute()`: the driver consumed
     * its result rows synchronously, so the collector holds the final status.
     */
    public void emitAfterExecute() {
        emit(collector.terminal() != null ? collector.terminal() : collector.latest());
    }

    /**
     * Emits the query stats once the result set rows were read, when the driver query is complete
     * (or as complete as the rows read so far, for `FETCH_ONE`).
     */
    public void emitFromResultSet(ResultSet resultSet) throws SQLException {
        if (resultSet instanceof TrinoResultSet trinoResultSet && !this.emitted) {
            emit(trinoResultSet.getStats());
        }
    }

    private void emit(QueryStats stats) {
        if (stats == null || emitted) {
            return;
        }
        emitted = true;

        runContext.metric(Counter.of("processed.rows", stats.getProcessedRows(), metricTags));
        runContext.metric(Counter.of("processed.bytes", stats.getProcessedBytes(), metricTags));
        runContext.metric(Counter.of("peak.memory.bytes", stats.getPeakMemoryBytes(), metricTags));
        runContext.metric(Counter.of("physical.written.bytes", stats.getPhysicalWrittenBytes(), metricTags));
        runContext.metric(Timer.of("cpu.time", Duration.ofMillis(stats.getCpuTimeMillis()), metricTags));
        runContext.metric(Timer.of("wall.time", Duration.ofMillis(stats.getWallTimeMillis()), metricTags));
        runContext.metric(Timer.of("queued.time", Duration.ofMillis(stats.getQueuedTimeMillis()), metricTags));

        runContext.logger().debug(
            "Trino query `{}` finished with state `{}`: {} rows processed, {} bytes processed, {} bytes peak memory",
            stats.getQueryId(), stats.getState(), stats.getProcessedRows(), stats.getProcessedBytes(), stats.getPeakMemoryBytes()
        );
    }

    /**
     * The base URL of the Web UI of the cluster, or `null` when the JDBC URL could not be parsed: the
     * coordinator serves every query page of a cluster at `host:port/ui/query.html?<query id>`, over TLS
     * when the URL connects with `SSL=true`.
     */
    private static String queryUiBase(String jdbcUrl, Boolean sslProperty) {
        if (jdbcUrl == null || !jdbcUrl.startsWith("jdbc:trino:")) {
            return null;
        }

        try {
            var serverUri = URI.create(jdbcUrl.substring("jdbc:trino:".length()));
            var authority = serverUri.getAuthority();
            if (authority == null || authority.isBlank()) {
                return null;
            }

            boolean ssl = sslProperty != null ? sslProperty : serverUri.getRawQuery() != null && java.util.Arrays.stream(serverUri.getRawQuery().split("&"))
                .anyMatch(pair -> pair.trim().toLowerCase().startsWith("ssl=true"));

            return (ssl ? "https" : "http") + "://" + authority + "/ui/";
        } catch (RuntimeException unused) {
            return null;
        }
    }
}
