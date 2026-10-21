package io.kestra.plugin.jdbc.sqlite;

import io.kestra.core.models.annotations.Plugin;
import io.kestra.plugin.jdbc.explorer.AbstractReadOnlyExplorerEndpoint;

@Plugin
public class SqliteCatalogEndpoint extends AbstractReadOnlyExplorerEndpoint {
    public SqliteCatalogEndpoint() {
        super(SqliteExplorerEngine.INSTANCE, Operation.CATALOG);
    }
}
