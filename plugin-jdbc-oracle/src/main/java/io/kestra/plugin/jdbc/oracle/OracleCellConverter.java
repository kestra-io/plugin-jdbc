package io.kestra.plugin.jdbc.oracle;

import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;
import lombok.SneakyThrows;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.TimeZone;

public class OracleCellConverter extends AbstractCellConverter {

    private static final int CLOB_BUFFER_SIZE = 4096;
    private static final int BLOB_BUFFER_SIZE = 8192;

    public OracleCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @SneakyThrows
    @SuppressWarnings("deprecation")
    @Override
    public Object convertCell(int columnIndex, ResultSet rs, Connection connection) {
        final Object data = rs.getObject(columnIndex);

        if (data == null) {
            return null;
        }

        if (data instanceof oracle.sql.BLOB blob) {
            try (InputStream inputStream = blob.getBinaryStream();
                 ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                byte[] buffer = new byte[BLOB_BUFFER_SIZE];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }
                return outputStream.toByteArray();
            } catch (Exception e) {
                throw new SQLException("Error reading BLOB data", e);
            }
        }

        if (data instanceof oracle.sql.CLOB clob) {
            try (Reader reader = clob.getCharacterStream();
                 StringWriter writer = new StringWriter()) {
                char[] buffer = new char[CLOB_BUFFER_SIZE];
                int bytesRead;
                while ((bytesRead = reader.read(buffer)) != -1) {
                    writer.write(buffer, 0, bytesRead);
                }
                return writer.toString();
            }
        }

        /*
        if (columnVal instanceof oracle.sql.BFILE) {
            oracle.sql.BFILE col = (oracle.sql.BFILE) columnVal;

            return ImmutableMap.of(
                "name", col.getName(),
                "bytes", IOUtils.toByteArray(col.getBinaryStream())
            );
        }
        */

        if (data instanceof oracle.sql.TIMESTAMP col) {
            return col.toLocalDateTime();
        }

        if (data instanceof oracle.sql.TIMESTAMPTZ col) {
            return col.toOffsetDateTime().toZonedDateTime();
        }

        if (data instanceof oracle.sql.TIMESTAMPLTZ col) {
            return col.toLocalDateTime(connection);
        }

        String columnTypeName = rs.getMetaData().getColumnTypeName(columnIndex);
        if (columnTypeName.equals("DATE")) {
            return ((Timestamp) data).toLocalDateTime();
        }

        return super.convert(columnIndex, rs);
    }

    @Override
    protected PreparedStatement addPreparedStatementValue(
        PreparedStatement ps,
        AbstractJdbcBatch.ParameterType parameterType,
        Object value,
        int index,
        Connection connection
    ) throws Exception {
        Class<?> cls = parameterType.getClass(index);

        if (cls == oracle.sql.TIMESTAMP.class || cls == oracle.sql.TIMESTAMPLTZ.class) {
            if (value instanceof LocalDateTime ldt) {
                ps.setTimestamp(index, Timestamp.valueOf(ldt));
                return ps;
            } else if (value instanceof ZonedDateTime zdt) {
                ps.setTimestamp(index, Timestamp.from(zdt.toInstant()));
                return ps;
            }
        }  else if (cls ==  oracle.sql.TIMESTAMPTZ.class) {
            if (value instanceof ZonedDateTime current) {
                ps.setTimestamp(index, Timestamp.valueOf(current.toLocalDateTime()), Calendar.getInstance(TimeZone.getTimeZone(current.getZone())));
                return ps;
            }
        }

        return super.addPreparedStatementValue(ps, parameterType, value, index, connection);
    }
}
