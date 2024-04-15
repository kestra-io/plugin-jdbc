package io.kestra.plugin.jdbc.as400;

import io.kestra.plugin.jdbc.AbstractCellConverter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;

/**
 * Copied from the DB2 code as we cannot test AS400 we assume it works like DB2
 */
public class As400CellConverter extends AbstractCellConverter {
    public As400CellConverter(ZoneId zoneId) {
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
		    case "char", "varchar" -> ((com.ibm.as400.access.AS400JDBCResultSet) rs).getString(columnIndex);
		    case "date" -> ((com.ibm.as400.access.AS400JDBCResultSet)  rs).getDate(columnIndex).toLocalDate();
		    case "time" -> ((com.ibm.as400.access.AS400JDBCResultSet)  rs).getTime(columnIndex).toLocalTime();
		    case "timestamp" -> ((com.ibm.as400.access.AS400JDBCResultSet) rs).getTimestamp(columnIndex).toInstant();
		    case "blob" -> ((com.ibm.as400.access.AS400JDBCResultSet)  rs).getBlob(columnIndex);
		    case "clob" -> ((com.ibm.as400.access.AS400JDBCResultSet) rs).getClob(columnIndex);
		    case "nclob" -> ((com.ibm.as400.access.AS400JDBCResultSet)  rs).getNClob(columnIndex);
		    case "xml" -> ((com.ibm.as400.access.AS400JDBCResultSet)  rs).getSQLXML(columnIndex);
		    default -> super.convert(columnIndex, rs);
	    };

    }
}
