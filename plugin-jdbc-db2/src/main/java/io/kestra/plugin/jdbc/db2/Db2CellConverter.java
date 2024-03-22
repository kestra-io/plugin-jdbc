package io.kestra.plugin.jdbc.db2;

import io.kestra.plugin.jdbc.AbstractCellConverter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;

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
		    case "char", "varchar" -> ((com.ibm.db2.jcc.am.ResultSet) rs).getString(columnIndex);
		    case "date" -> ((com.ibm.db2.jcc.am.ResultSet) rs).getDate(columnIndex).toLocalDate();
		    case "time" -> ((com.ibm.db2.jcc.am.ResultSet) rs).getTime(columnIndex).toLocalTime();
		    case "timestamp" -> ((com.ibm.db2.jcc.am.ResultSet) rs).getTimestamp(columnIndex).toInstant();
		    case "blob" -> ((com.ibm.db2.jcc.am.ResultSet) rs).getBlob(columnIndex);
		    case "clob" -> ((com.ibm.db2.jcc.am.ResultSet) rs).getClob(columnIndex);
		    case "nclob" -> ((com.ibm.db2.jcc.am.ResultSet) rs).getNClob(columnIndex);
		    case "xml" -> ((com.ibm.db2.jcc.am.ResultSet) rs).getSQLXML(columnIndex);
		    default -> super.convert(columnIndex, rs);
	    };

    }
}
