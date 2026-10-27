package io.kestra.plugin.jdbc.sqlite;

import io.kestra.core.models.annotations.Plugin;
import io.kestra.plugin.jdbc.explorer.AbstractReadOnlyExplorerEndpoint;

@Plugin
public class SqliteQueryEndpoint extends AbstractReadOnlyExplorerEndpoint {
    public SqliteQueryEndpoint() {
        super(SqliteExplorerEngine.INSTANCE, Operation.QUERY);
    }
}
