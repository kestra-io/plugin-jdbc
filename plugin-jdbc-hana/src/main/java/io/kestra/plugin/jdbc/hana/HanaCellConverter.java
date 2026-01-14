package io.kestra.plugin.jdbc.hana;

import io.kestra.plugin.jdbc.AbstractCellConverter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;



public class HanaCellConverter extends AbstractCellConverter {

    public HanaCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @Override
    public Object convertCell(int columnIndex, ResultSet rs, Connection connection) throws SQLException {
        Object data = rs.getObject(columnIndex);
        if (data == null) {
            return null;
        }

        // SAP HANA uses standard type names; handle common conversions:
        String type = rs.getMetaData().getColumnTypeName(columnIndex).toLowerCase();

        return switch (type) {
            case "nvarchar", "varchar", "alphanum", "shorttext" -> rs.getString(columnIndex);
            case "date" -> rs.getDate(columnIndex).toLocalDate();
            case "time" -> rs.getTime(columnIndex).toLocalTime();
            case "timestamp" -> rs.getTimestamp(columnIndex).toInstant();
            case "blob" -> rs.getBlob(columnIndex);
            case "clob" -> rs.getClob(columnIndex);
            case "nclob" -> rs.getNClob(columnIndex);
            case "varbinary", "binary" -> rs.getBytes(columnIndex);
            default -> super.convert(columnIndex, rs);
        };
    }
}
