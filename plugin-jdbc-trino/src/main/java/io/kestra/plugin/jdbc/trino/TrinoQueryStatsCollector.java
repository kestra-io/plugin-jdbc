package io.kestra.plugin.jdbc.trino;

import io.trino.jdbc.QueryStats;

import java.util.Set;
import java.util.function.Consumer;

/**
 * Keeps the last query stats the Trino JDBC driver reported for the statement it is attached to.
 *
 * <p>
 * The driver pushes a status update for every page fetched while results are read, and terminates with a
 * final update once the engine completed the query (see {@code io.trino.jdbc.AsyncResultIterator}), so the
 * collector holds a {@link #latest()} snapshot while the query runs and a {@link #terminal()} one once the
 * query completed, whichever path the results followed (statement results read by the driver, or rows
 * streamed to this plugin).
 */
public final class TrinoQueryStatsCollector implements Consumer<QueryStats> {

    // `QueryStats.getState()` reports the `QueryState` name of the trino client.
    private static final Set<String> TERMINAL_STATES = Set.of("FINISHED", "FAILED", "CANCELED");

    private QueryStats latest;
    private QueryStats terminal;

    @Override
    public synchronized void accept(QueryStats stats) {
        if (stats == null) {
            return;
        }
        this.latest = stats;
        if (this.terminal == null && TERMINAL_STATES.contains(stats.getState())) {
            this.terminal = stats;
        }
    }

    /** Drops the stats of the collected statement, called when the collector is attached to a new statement. */
    public synchronized void reset() {
        this.latest = null;
        this.terminal = null;
    }

    public synchronized QueryStats latest() {
        return latest;
    }

    public synchronized QueryStats terminal() {
        return terminal;
    }

    /** The engine query id the stats were collected for, or `null` while no status was received. */
    public synchronized String queryId() {
        var stats = latest != null ? latest : terminal;
        return stats != null ? stats.getQueryId() : null;
    }
}
