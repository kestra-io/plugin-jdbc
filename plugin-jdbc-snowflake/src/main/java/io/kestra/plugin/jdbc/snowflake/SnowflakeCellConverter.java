package io.kestra.plugin.jdbc.snowflake;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.List;

public class SnowflakeCellConverter extends AbstractCellConverter {
    static final protected ObjectMapper MAPPER = JacksonMapper.ofJson();
    private static final List<String> SNOWFLAKE_TIME_WITH_TIMEZONE_CLASS_NAMES = List.of(
        "net.snowflake.client.jdbc.SnowflakeTimeWithTimezone",
        "net.snowflake.client.internal.jdbc.SnowflakeTimeWithTimezone"
    );
    private static final List<String> SNOWFLAKE_TIMESTAMP_WITH_TIMEZONE_CLASS_NAMES = List.of(
        "net.snowflake.client.jdbc.SnowflakeTimestampWithTimezone",
        "net.snowflake.client.internal.jdbc.SnowflakeTimestampWithTimezone"
    );
    private static final List<String> SNOWFLAKE_DATE_WITH_TIMEZONE_CLASS_NAMES = List.of(
        "net.snowflake.client.jdbc.SnowflakeDateWithTimezone",
        "net.snowflake.client.internal.jdbc.SnowflakeDateWithTimezone"
    );

    public SnowflakeCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @SneakyThrows
    @Override
    public Object convertCell(int columnIndex, ResultSet rs, Connection connection) throws SQLException {
        var data = rs.getObject(columnIndex);

        if (data == null) {
            return null;
        }

        var columnVal = rs.getObject(columnIndex);
        var columnTypeName = rs.getMetaData().getColumnTypeName(columnIndex);

        if (isSnowflakeType(columnVal, SNOWFLAKE_TIME_WITH_TIMEZONE_CLASS_NAMES) && columnVal instanceof Time time) {
            return time.toLocalTime();
        }

        if (isSnowflakeType(columnVal, SNOWFLAKE_TIMESTAMP_WITH_TIMEZONE_CLASS_NAMES) && columnVal instanceof Timestamp timestamp) {
            return timestamp.toInstant();
        }

        if (isSnowflakeType(columnVal, SNOWFLAKE_DATE_WITH_TIMEZONE_CLASS_NAMES) && columnVal instanceof Date date) {
            return date.toLocalDate();
        }

        if (List.of("VARIANT", "OBJECT", "ARRAY", "GEOGRAPHY").contains(columnTypeName)) {
            return MAPPER.readValue(((String) columnVal).replaceAll("\\sundefined\\n", "null"), Object.class);
        }

        return super.convert(columnIndex, rs);
    }

    private static boolean isSnowflakeType(Object value, List<String> classNames) {
        return classNames.contains(value.getClass().getName());
    }
}
