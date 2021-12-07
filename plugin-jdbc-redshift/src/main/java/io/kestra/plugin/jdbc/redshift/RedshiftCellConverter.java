package io.kestra.plugin.jdbc.redshift;

import com.amazon.redshift.util.RedshiftTimestamp;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.jdbc.AbstractCellConverter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.GregorianCalendar;

public class RedshiftCellConverter extends AbstractCellConverter {
    public RedshiftCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @Override
    public Object convertCell(int columnIndex, ResultSet rs, Connection connection) throws SQLException {
        Object data = rs.getObject(columnIndex);

        if (data == null) {
            return null;
        }

        Object columnVal = rs.getObject(columnIndex);
        String columnTypeName = rs.getMetaData().getColumnTypeName(columnIndex);
        String columnName = rs.getMetaData().getColumnName(columnIndex);

        if (columnVal instanceof RedshiftTimestamp) {
            RedshiftTimestamp col = (RedshiftTimestamp) columnVal;
            return ((GregorianCalendar) col.getCalendar()).toZonedDateTime();
        }

        if (columnTypeName.equals("super")) {
            try {
                return JacksonMapper.ofJson(false).readValue((String) columnVal, Object.class);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Invalid data for 'super' column '" + columnName + "' " + e.getMessage(), e);
            }
        }

        if (columnTypeName.equals("timetz")) {
            java.sql.Time col = ((java.sql.Time) columnVal);
            return col.toLocalTime().atOffset(zoneId.getRules().getOffset(Instant.now()));
        }

        if (columnTypeName.equals("timestamp")) {
            return ((Timestamp) data).toLocalDateTime();
        }

        return super.convert(columnIndex, rs);
    }
}
