package io.kestra.plugin.jdbc.openedge;

import io.kestra.plugin.jdbc.AbstractCellConverter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Locale;

public class OpenEdgeCellConverter extends AbstractCellConverter {
    public OpenEdgeCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @Override
    public Object convertCell(int columnIndex, ResultSet rs, Connection connection) throws SQLException {
        Object data = rs.getObject(columnIndex);
        if (data == null) {
            return null;
        }

        String columnTypeName = rs.getMetaData().getColumnTypeName(columnIndex);
        return switch (columnTypeName.toLowerCase(Locale.ROOT)) {
            case "time" -> rs.getTime(columnIndex).toLocalTime();
            // OpenEdge ABL DATETIME is exposed by the SQL/JDBC driver as TIMESTAMP.
            case "datetime", "timestamp" -> rs.getTimestamp(columnIndex).toLocalDateTime();
            default -> super.convert(columnIndex, rs);
        };
    }
}
