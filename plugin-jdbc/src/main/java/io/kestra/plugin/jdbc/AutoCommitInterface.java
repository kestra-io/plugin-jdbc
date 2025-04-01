package io.kestra.plugin.jdbc;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

public interface AutoCommitInterface {
    @Deprecated(since = "0.22.0", forRemoval = true)
    @Schema(
        title = "DEPRECATED Whether autocommit is enabled.",
        description = "Sets this connection's auto-commit mode to the given state. If a connection is in auto-commit " +
            "mode, then all its SQL statements will be executed and committed as individual transactions. Otherwise, " +
            "its SQL statements are grouped into transactions that are terminated by a call to either the method commit " +
            "or the method rollback. By default, new connections are in auto-commit mode except when you are using " +
            "`store` property in which case the auto-commit will be disabled." +
            "\nDEPRECATED: Please use Queries task with 'transaction' property if you want to run multiple queries with or without autocommit",
        deprecated = true
    )
    Property<Boolean> getAutoCommit();
}
