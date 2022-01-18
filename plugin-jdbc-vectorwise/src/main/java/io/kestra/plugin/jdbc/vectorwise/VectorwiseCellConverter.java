package io.kestra.plugin.jdbc.vectorwise;

import io.kestra.plugin.jdbc.AbstractCellConverter;


import java.sql.*;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;

public class VectorwiseCellConverter extends AbstractCellConverter {
    public VectorwiseCellConverter(ZoneId zoneId) {
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

        if (columnTypeName.equals("timetz")) {
            java.sql.Time col = ((java.sql.Time) columnVal);
            return col.toLocalTime().atOffset(zoneId.getRules().getOffset(Instant.now()));
        }

        if (columnTypeName.equals("timestamp without time zone")) {
            return ((Timestamp) data).toLocalDateTime();
        }

        if (columnTypeName.equals("time with time zone")) {
            // FIXME : Since Time uses 01-01-1970 by default, timezone needs to be adjusted
            return LocalTime.from(rs.getTimestamp(columnIndex).toInstant().atZone(zoneId));
        }

        if (columnTypeName.equals("timestamp")) {
            return rs.getTimestamp(columnIndex).toLocalDateTime();
        }

        if (columnTypeName.equals("timestamp with time zone")) {
            return rs.getTimestamp(columnIndex).toInstant().atZone(zoneId);
        }

        return super.convert(columnIndex, rs);
    }

    public PreparedStatement adaptedStatement(PreparedStatement ps, Object prop, int index, Connection connection) throws Exception {
        return this.adaptStatement(ps, prop, index, connection);
    }
}
