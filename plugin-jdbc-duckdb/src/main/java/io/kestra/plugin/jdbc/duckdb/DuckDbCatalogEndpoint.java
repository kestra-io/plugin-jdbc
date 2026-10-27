package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.models.annotations.Plugin;
import io.kestra.plugin.jdbc.explorer.AbstractReadOnlyExplorerEndpoint;

@Plugin
public class DuckDbCatalogEndpoint extends AbstractReadOnlyExplorerEndpoint {
    public DuckDbCatalogEndpoint() {
        super(DuckDbExplorerEngine.INSTANCE, Operation.CATALOG);
    }
}
