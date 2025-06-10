package io.kestra.plugin.jdbc;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.ZoneId;
import java.util.TimeZone;

public interface JdbcStatementInterface extends JdbcConnectionInterface {
    @Schema(
        title = "The time zone id to use for date/time manipulation. Default value is the worker's default time zone id."
    )
    @PluginProperty(group = "connection")
    Property<String> getTimeZoneId();

    default ZoneId zoneId(RunContext runContext) throws IllegalVariableEvaluationException {
        if (this.getTimeZoneId() != null) {
            return ZoneId.of(runContext.render(this.getTimeZoneId()).as(String.class).orElseThrow());
        }

        return TimeZone.getDefault().toZoneId();
    }
}
