package io.kestra.plugin.jdbc.trino;

import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;

import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.runners.RunContext;

import io.trino.jdbc.QueryStats;
import io.trino.jdbc.TrinoResultSet;
import io.trino.jdbc.TrinoStatement;

/**
 * Exposes the Trino query id of each statement and emits its driver query stats as task metrics, once per statement.
 * Not thread-safe: used from the task run thread only, the collector is the only part the driver calls.
 */
public final class TrinoQueryMonitor {

    private final RunContext runContext;
    private final String[] metricTags;
    private final String webUiBase;
    private final TrinoQueryStatsCollector collector = new TrinoQueryStatsCollector();

    private boolean emitted;

    public TrinoQueryMonitor(RunContext runContext, String jdbcUrl, Boolean ssl, String... metricTags) {
        this.runContext = runContext;
        this.metricTags = metricTags;
        this.webUiBase = queryUiBase(jdbcUrl, ssl);
    }

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
        var queryId = collector.queryId();
        logQuery(queryId);
        return queryId;
    }

    /** Emits the latest available stats without draining a discarded result set. */
    public void discardResults() {
        emit(collector.latest());
    }

    private void logQuery(String queryId) {
        if (queryId == null) {
            return;
        }
        var webUiUrl = webUiBase != null ? "%squery.html?%s".formatted(webUiBase, queryId) : null;
        if (webUiUrl != null) {
            runContext.logger().info("Trino query executed with id `{}`, details in the Trino Web UI at `{}`", queryId, webUiUrl);
        } else {
            runContext.logger().info("Trino query executed with id `{}`", queryId);
        }
    }

    // A statement without results completes during execute(), so the collector already holds the final status.
    private void emitAfterExecute() {
        emit(collector.terminal() != null ? collector.terminal() : collector.latest());
    }

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

            var ssl = sslProperty != null ? sslProperty
                : serverUri.getRawQuery() != null && Arrays.stream(serverUri.getRawQuery().split("&"))
                    .anyMatch(pair -> pair.trim().toLowerCase().startsWith("ssl=true"));

            return "%s://%s/ui/".formatted(ssl ? "https" : "http", authority);
        } catch (RuntimeException unused) {
            return null;
        }
    }
}
