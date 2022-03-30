package io.kestra.plugin.jdbc.snowflake;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import lombok.SneakyThrows;
import net.snowflake.client.jdbc.SnowflakeDateWithTimezone;
import net.snowflake.client.jdbc.SnowflakeTimeWithTimezone;
import net.snowflake.client.jdbc.SnowflakeTimestampWithTimezone;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.List;

public class SnowflakeCellConverter extends AbstractCellConverter {
    static final protected ObjectMapper MAPPER = JacksonMapper.ofJson();

    public SnowflakeCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @SneakyThrows
    @Override
    public Object convertCell(int columnIndex, ResultSet rs, Connection connection) throws SQLException {
        Object data = rs.getObject(columnIndex);

        if (data == null) {
            return null;
        }

        Object columnVal = rs.getObject(columnIndex);
        String columnTypeName = rs.getMetaData().getColumnTypeName(columnIndex);

        if (columnVal instanceof SnowflakeTimeWithTimezone) {
            SnowflakeTimeWithTimezone col = (SnowflakeTimeWithTimezone) columnVal;
            return col.toLocalTime();
        }

        if (columnVal instanceof SnowflakeTimestampWithTimezone) {
            SnowflakeTimestampWithTimezone col = (SnowflakeTimestampWithTimezone) columnVal;
            return col.toInstant();
        }

        if (columnVal instanceof SnowflakeDateWithTimezone) {
            SnowflakeDateWithTimezone col = (SnowflakeDateWithTimezone) columnVal;
            return col.toLocalDate();
        }

        if (List.of("VARIANT", "OBJECT", "ARRAY", "GEOGRAPHY").contains(columnTypeName)) {
            return MAPPER.readValue(((String) columnVal).replaceAll("\\sundefined\\n", "null"), Object.class);
        }

        return super.convert(columnIndex, rs);
    }
}
