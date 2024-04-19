package io.kestra.plugin.jdbc.sybase;

import io.kestra.plugin.jdbc.AbstractCellConverter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class SybaseCellConverter extends AbstractCellConverter {
    public SybaseCellConverter(ZoneId zoneId) {
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
		    case "time" -> rs.getTime(columnIndex).toLocalTime();
		    case "datetime" -> LocalDateTime.of(rs.getDate(columnIndex).toLocalDate(), rs.getTime(columnIndex).toLocalTime());
		    default -> super.convert(columnIndex, rs);
	    };

    }
}
