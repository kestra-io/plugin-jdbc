package io.kestra.plugin.jdbc;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.Map;

public interface JdbcQueriesInterface {
    @Schema(
        title = "Parameters",
        description = "A map of parameters to bind to the SQL queries. The keys should match the parameter placeholders in the SQL string, e.g., :parameterName."
    )
    Property<Map<String, String>> getParameters();

    @Schema(
        title = "Transaction",
        description = "If one query failed, rollback transactions."
    )
    Property<Boolean> getTransaction();
}
