package io.kestra.plugin.jdbc.sqlserver;

import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;
import lombok.SneakyThrows;

import java.sql.*;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.TimeZone;

public class SqlServerCellConverter extends AbstractCellConverter {
    public SqlServerCellConverter(ZoneId zoneId) {
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

        if (columnVal instanceof microsoft.sql.DateTimeOffset) {
            microsoft.sql.DateTimeOffset col = (microsoft.sql.DateTimeOffset) columnVal;
            return col.getOffsetDateTime();
        }


        return super.convert(columnIndex, rs);
    }

    @Override
    protected PreparedStatement addPreparedStatementValue(
        PreparedStatement ps,
        AbstractJdbcBatch.ParameterType parameterType,
        Object value,
        int index,
        Connection connection
    ) throws Exception {
        Class<?> cls = parameterType.getClass(index);
        String typeName = parameterType.getTypeName(index);

        if (cls ==  microsoft.sql.DateTimeOffset.class) {
            if (value instanceof OffsetDateTime) {
                OffsetDateTime current = ((OffsetDateTime) value);
                ps.setTimestamp(index, Timestamp.valueOf(current.toLocalDateTime()), Calendar.getInstance(TimeZone.getTimeZone(current.toZonedDateTime().getZone())));
                return ps;
            }
        }

        return super.addPreparedStatementValue(ps, parameterType, value, index, connection);
    }
}
