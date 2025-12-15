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
            case "blob" -> rs.getBlob(columnIndex);
            case "clob" -> rs.getClob(columnIndex);
            case "nclob" -> rs.getNClob(columnIndex);
            case "xml" -> rs.getSQLXML(columnIndex);
            default -> super.convert(columnIndex, rs);
        };
    }
}
