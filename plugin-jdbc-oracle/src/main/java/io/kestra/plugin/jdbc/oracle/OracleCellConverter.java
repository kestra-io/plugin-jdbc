package io.kestra.plugin.jdbc.oracle;

import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;
import lombok.SneakyThrows;
import oracle.jdbc.OracleBlob;
import oracle.jdbc.OracleClob;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleNClob;
import org.apache.commons.lang3.time.DurationFormatUtils;

import java.sql.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.TimeZone;

public class OracleCellConverter extends AbstractCellConverter {
    public OracleCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @SneakyThrows
    @SuppressWarnings("deprecation")
    @Override
    public Object convertCell(int columnIndex, ResultSet rs, Connection connection) throws SQLException {
        Object data = rs.getObject(columnIndex);

        if (data == null) {
            return null;
        }

        Object columnVal = rs.getObject(columnIndex);
        String columnTypeName = rs.getMetaData().getColumnTypeName(columnIndex);

        if (columnVal instanceof oracle.sql.BLOB) {
            oracle.sql.BLOB col = (oracle.sql.BLOB) columnVal;
            return col.getBytes(1, Long.valueOf(col.length()).intValue());
        }

        if (columnVal instanceof oracle.sql.CLOB) {
            oracle.sql.CLOB col = (oracle.sql.CLOB) columnVal;
            return col.getSubString(1, (int) col.length());
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

        if (columnVal instanceof oracle.sql.TIMESTAMP) {
            oracle.sql.TIMESTAMP col = (oracle.sql.TIMESTAMP) columnVal;
            return col.toLocalDateTime();
        }

        if (columnVal instanceof oracle.sql.TIMESTAMPTZ) {
            oracle.sql.TIMESTAMPTZ col = (oracle.sql.TIMESTAMPTZ) columnVal;
            return col.toOffsetDateTime().toZonedDateTime();
        }

        if (columnVal instanceof oracle.sql.TIMESTAMPLTZ) {
            oracle.sql.TIMESTAMPLTZ col = (oracle.sql.TIMESTAMPLTZ) columnVal;

            return col.toLocalDateTime(connection);
        }

        if (columnTypeName.equals("DATE")) {
            return ((Timestamp) data).toLocalDateTime().toLocalDate();
        }


//        if (columnVal instanceof ClickHouseArray) {
//            ClickHouseArray col = (ClickHouseArray) columnVal;
//            return col.getArray();
//        }
//
//        if (columnTypeName.startsWith("DateTime")) {
//            Matcher matcher = PATTERN.matcher(columnTypeName);
//            if (!matcher.find() || matcher.groupCount() < 3) {
//                throw new IllegalArgumentException("Invalid Column Type '" + columnTypeName + "'");
//            }
//
//            return LocalDateTime
//                .parse(
//                    rs.getString(columnIndex),
//                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]")
//                )
//                .atZone(ZoneId.of(matcher.group(3)))
//                .withZoneSameInstant(zoneId);
//        }

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

        if (cls ==  oracle.sql.TIMESTAMP.class) {
            if (value instanceof LocalDateTime) {
                ps.setTimestamp(index, Timestamp.valueOf((LocalDateTime) value));
                return ps;
            }
        } else if (cls ==  oracle.sql.TIMESTAMPTZ.class) {
            if (value instanceof ZonedDateTime) {
                ZonedDateTime current = (ZonedDateTime) value;

                ps.setTimestamp(index, Timestamp.valueOf(current.toLocalDateTime()), Calendar.getInstance(TimeZone.getTimeZone(current.getZone())));
                return ps;
            }
        } else if (cls ==  oracle.sql.TIMESTAMPLTZ.class) {
            if (value instanceof LocalDateTime) {
                ps.setTimestamp(index, Timestamp.valueOf((LocalDateTime) value));
                return ps;
            }
        }

        return super.addPreparedStatementValue(ps, parameterType, value, index, connection);
    }
}
