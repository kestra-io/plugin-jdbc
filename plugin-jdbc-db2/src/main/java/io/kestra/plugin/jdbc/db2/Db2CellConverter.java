package io.kestra.plugin.jdbc.db2;

import io.kestra.plugin.jdbc.AbstractCellConverter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.TimeZone;

public class Db2CellConverter extends AbstractCellConverter {
    public Db2CellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @Override
    public Object convertCell(int columnIndex, ResultSet rs, Connection connection) throws SQLException {
        Object data = rs.getObject(columnIndex);

        if (data == null) {
            return null;
        }

        String columnTypeName = rs.getMetaData().getColumnTypeName(columnIndex);

        return switch (columnTypeName.toLowerCase()) {
            case "char", "varchar" -> rs.getString(columnIndex);
            case "date" -> rs.getDate(columnIndex).toLocalDate();
            case "time" -> rs.getTime(columnIndex).toLocalTime();
            case "timestamp" -> {
                ZoneId zid = this.zoneId != null ? this.zoneId : ZoneId.of("UTC");
                var cal = Calendar.getInstance(TimeZone.getTimeZone(zid));
                var ts = rs.getTimestamp(columnIndex, cal);
                yield ts == null ? null : ts.toInstant();
            }
            // Blob/Clob/NClob/SQLXML are live locators backed by the ResultSet's connection: reading
            // them lazily (after the row is out of scope, e.g. once rows are batched for downstream
            // processing) throws once the underlying statement/connection has been closed or advanced.
            // Materialize the actual content here, while the ResultSet is still positioned on this row.
            case "blob" -> readBlob(rs.getBlob(columnIndex));
            case "clob" -> readClob(rs.getClob(columnIndex));
            case "nclob" -> readNClob(rs.getNClob(columnIndex));
            case "xml" -> readSqlXml(rs.getSQLXML(columnIndex));
            default -> super.convert(columnIndex, rs);
        };
    }
}
