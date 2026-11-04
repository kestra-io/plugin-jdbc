package io.kestra.plugin.jdbc.trino;

import java.util.Set;
import java.util.function.Consumer;

import io.trino.jdbc.QueryStats;

/** Keeps the latest and the terminal query stats the Trino driver reports, from its own thread. */
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

    /** The query id, or null while no status was received. */
    public synchronized String queryId() {
        return latest != null ? latest.getQueryId() : null;
    }
}
