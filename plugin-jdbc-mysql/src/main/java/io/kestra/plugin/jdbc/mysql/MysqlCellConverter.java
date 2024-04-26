package io.kestra.plugin.jdbc.mysql;

import com.mysql.cj.jdbc.result.ResultSetImpl;
import io.kestra.plugin.jdbc.AbstractCellConverter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.*;

public class MysqlCellConverter extends AbstractCellConverter {
    public MysqlCellConverter(ZoneId zoneId) {
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
		    case "time" -> ((ResultSetImpl) rs).getLocalTime(columnIndex);
		    case "datetime" -> rs.getTimestamp(columnIndex).toLocalDateTime();
		    case "timestamp" -> ((ResultSetImpl) rs).getLocalDateTime(columnIndex).toInstant(ZoneOffset.UTC);
		    default -> super.convert(columnIndex, rs);
	    };

    }
}
