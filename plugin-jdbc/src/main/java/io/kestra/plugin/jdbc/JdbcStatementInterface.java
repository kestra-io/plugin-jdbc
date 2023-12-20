package io.kestra.plugin.jdbc;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.ZoneId;
import java.util.TimeZone;

public interface JdbcStatementInterface extends JdbcConnectionInterface {
    @Schema(
        title = "The time zone id to use for date/time manipulation. Default value is the worker's default time zone id."
    )
    @PluginProperty(dynamic = false)
    String getTimeZoneId();

    default ZoneId zoneId() {
        if (this.getTimeZoneId() != null) {
            return ZoneId.of(this.getTimeZoneId());
        }

        return TimeZone.getDefault().toZoneId();
    }
}
