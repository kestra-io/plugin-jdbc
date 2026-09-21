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
            // Blob/Clob/NClob are live locators backed by the ResultSet's connection: reading them
            // lazily (after the row is out of scope, e.g. once rows are batched for downstream
            // processing) throws once the underlying statement/connection has been closed or
            // advanced. Materialize the actual content here, while the ResultSet is still
            // positioned on this row (same fix as Db2CellConverter, #1005).
            case "blob" -> readBlob(rs.getBlob(columnIndex));
            case "clob" -> readClob(rs.getClob(columnIndex));
            case "nclob" -> readNClob(rs.getNClob(columnIndex));
            case "varbinary", "binary" -> rs.getBytes(columnIndex);
            default -> super.convert(columnIndex, rs);
        };
    }
}
