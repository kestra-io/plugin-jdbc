package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.models.annotations.Plugin;
import io.kestra.plugin.jdbc.explorer.AbstractReadOnlyExplorerEndpoint;

@Plugin
public class DuckDbQueryEndpoint extends AbstractReadOnlyExplorerEndpoint {
    public DuckDbQueryEndpoint() {
        super(DuckDbExplorerEngine.INSTANCE, Operation.QUERY);
    }
}
