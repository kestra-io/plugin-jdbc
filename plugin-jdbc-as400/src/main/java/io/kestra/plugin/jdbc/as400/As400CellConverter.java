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
		    // Blob/Clob/NClob/SQLXML are live locators backed by the ResultSet's connection: reading
		    // them lazily (after the row is out of scope, e.g. once rows are batched for downstream
		    // processing) throws once the underlying statement/connection has been closed or
		    // advanced. Materialize the actual content here, while the ResultSet is still positioned
		    // on this row -- this file's own comment above says it assumed AS400 works like DB2, but
		    // DB2 no longer works this way since #1005 fixed the identical bug there.
		    case "blob" -> readBlob(((com.ibm.as400.access.AS400JDBCResultSet) rs).getBlob(columnIndex));
		    case "clob" -> readClob(((com.ibm.as400.access.AS400JDBCResultSet) rs).getClob(columnIndex));
		    case "nclob" -> readNClob(((com.ibm.as400.access.AS400JDBCResultSet) rs).getNClob(columnIndex));
		    case "xml" -> readSqlXml(((com.ibm.as400.access.AS400JDBCResultSet) rs).getSQLXML(columnIndex));
		    default -> super.convert(columnIndex, rs);
	    };

    }
}
