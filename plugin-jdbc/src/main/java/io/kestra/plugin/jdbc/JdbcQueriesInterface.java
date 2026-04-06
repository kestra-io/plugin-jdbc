package io.kestra.plugin.jdbc;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.Map;
import io.kestra.core.models.annotations.PluginProperty;

public interface JdbcQueriesInterface {
    @Schema(
        title = "Transaction",
        description = "If one query failed, rollback transactions."
    )
    @PluginProperty(group = "advanced")
    Property<Boolean> getTransaction();
}
