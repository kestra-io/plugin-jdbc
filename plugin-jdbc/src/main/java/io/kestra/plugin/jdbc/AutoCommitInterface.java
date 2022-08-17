package io.kestra.plugin.jdbc;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

public interface AutoCommitInterface {
    @Schema(
        title = "If autocommit is enabled",
        description = "Sets this connection's auto-commit mode to the given state. If a connection is in auto-commit " +
            "mode, then all its SQL statements will be executed and committed as individual transactions. Otherwise, " +
            "its SQL statements are grouped into transactions that are terminated by a call to either the method commit" +
            "or the method rollback. By default, new connections are in auto-commit mode except if you are using a " +
            "`store` properties that will disabled autocommit whenever this properties values."
    )
    @PluginProperty(dynamic = false)
    Boolean getAutoCommit();
}
