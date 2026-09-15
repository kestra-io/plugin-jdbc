package io.kestra.plugin.jdbc.as400;

import io.kestra.plugin.jdbc.AbstractCellConverter;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.time.ZoneId;

/**
 * Copied from the DB2 code as we cannot test AS400 we assume it works like DB2
 */
public class As400CellConverter extends AbstractCellConverter {

    private static final int LOB_BUFFER_SIZE = 8192;

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

    private static byte[] readBlob(Blob blob) throws SQLException {
        if (blob == null) {
            return null;
        }
        try (InputStream inputStream = blob.getBinaryStream();
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[LOB_BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            return outputStream.toByteArray();
        } catch (java.io.IOException e) {
            throw new SQLException("Error reading BLOB data", e);
        } finally {
            blob.free();
        }
    }

    private static String readClob(Clob clob) throws SQLException {
        if (clob == null) {
            return null;
        }
        try (Reader reader = clob.getCharacterStream();
             StringWriter writer = new StringWriter()) {
            char[] buffer = new char[LOB_BUFFER_SIZE];
            int charsRead;
            while ((charsRead = reader.read(buffer)) != -1) {
                writer.write(buffer, 0, charsRead);
            }
            return writer.toString();
        } catch (java.io.IOException e) {
            throw new SQLException("Error reading CLOB data", e);
        } finally {
            clob.free();
        }
    }

    private static String readNClob(NClob nclob) throws SQLException {
        if (nclob == null) {
            return null;
        }
        try (Reader reader = nclob.getCharacterStream();
             StringWriter writer = new StringWriter()) {
            char[] buffer = new char[LOB_BUFFER_SIZE];
            int charsRead;
            while ((charsRead = reader.read(buffer)) != -1) {
                writer.write(buffer, 0, charsRead);
            }
            return writer.toString();
        } catch (java.io.IOException e) {
            throw new SQLException("Error reading NCLOB data", e);
        } finally {
            nclob.free();
        }
    }

    private static String readSqlXml(SQLXML sqlxml) throws SQLException {
        if (sqlxml == null) {
            return null;
        }
        try {
            return sqlxml.getString();
        } finally {
            sqlxml.free();
        }
    }
}
