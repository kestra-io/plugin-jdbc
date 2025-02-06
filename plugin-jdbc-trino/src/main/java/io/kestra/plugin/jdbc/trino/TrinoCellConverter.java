package io.kestra.plugin.jdbc.trino;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.trino.jdbc.TrinoIntervalDayTime;
import io.trino.jdbc.TrinoIntervalYearMonth;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;

public class TrinoCellConverter extends AbstractCellConverter {
    public TrinoCellConverter(ZoneId zoneId) {
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
        String columnName = rs.getMetaData().getColumnLabel(columnIndex);

        if (columnTypeName.equals("tinyint")) {
            Byte col = (Byte) columnVal;
            return col.intValue();
        }

        if (columnTypeName.equals("json")) {
            try {
                return JacksonMapper.ofJson(false).readValue((String) columnVal, Object.class);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Invalid data for 'super' column '" + columnName + "' " + e.getMessage(), e);
            }
        }

        if (columnVal instanceof TrinoIntervalYearMonth) {
            TrinoIntervalYearMonth col = (TrinoIntervalYearMonth) columnVal;
            return "P" + col.getMonths() + "M";
        }

        if (columnVal instanceof TrinoIntervalDayTime) {
            TrinoIntervalDayTime col = (TrinoIntervalDayTime) columnVal;
            return Duration.ofMillis(col.getMilliSeconds()).toString();
        }

        if (columnTypeName.startsWith("time with time zone")) {
            return OffsetTime.parse(rs.getString(columnIndex));
        }

        if (columnTypeName.startsWith("timestamp") && !columnTypeName.contains("with time zone")) {
            return LocalDateTime.parse(rs.getString(columnIndex).replace(" ", "T"));
        }

        return super.convert(columnIndex, rs);
    }
}
