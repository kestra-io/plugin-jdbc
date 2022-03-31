package io.kestra.plugin.jdbc;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.time.ZoneId;
import java.util.TimeZone;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJdbcStatement extends AbstractJdbcConnection {
    @Schema(
        title = "The time zone id to use for date/time manipulation. Default value is the worker default zone id."
    )
    private String timeZoneId;

    protected ZoneId zoneId() {
        ZoneId zoneId = TimeZone.getDefault().toZoneId();
        if (this.timeZoneId != null) {
            zoneId = ZoneId.of(timeZoneId);
        }

        return zoneId;
    }
}
