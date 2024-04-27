package io.kestra.plugin.jdbc.mysql;

import com.mysql.cj.jdbc.result.ResultSetImpl;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.*;
import java.util.Calendar;
import java.util.Collection;
import java.util.TimeZone;
import java.util.UUID;

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

    @Override
    public PreparedStatement addPreparedStatementValue(PreparedStatement ps, AbstractJdbcBatch.ParameterType parameterType, Object value, int index, Connection connection) throws Exception {
        if (value == null) {
            ps.setNull(index, parameterType.getType(index));
            return ps;
        }

        Class<?> cls = value.getClass();

        try {
            switch (cls.getSimpleName()) {
                case "Integer", "int" -> ps.setInt(index, (Integer) value);
                case "Short", "short" -> ps.setShort(index, Short.parseShort(value.toString()));
                case "String" -> ps.setString(index, (String) value);
                case "UUID" -> ps.setObject(index, value);
                case "Long", "long", "BigInteger" -> ps.setLong(index, ((Number) value).longValue());
                case "Double", "double" -> ps.setDouble(index, (Double) value);
                case "Float", "float" -> ps.setFloat(index, (Float) value);
                case "BigDecimal" -> ps.setBigDecimal(index, (BigDecimal) value);
                case "LocalDate" -> ps.setDate(index, Date.valueOf((LocalDate) value));
                case "LocalTime" -> ps.setTime(index, Time.valueOf((LocalTime) value));
                case "OffsetTime" -> {
                    OffsetTime current = (OffsetTime) value;
                    ps.setTime(index, Time.valueOf(current.toLocalTime()), Calendar.getInstance(TimeZone.getTimeZone(current.getOffset())));
                }
                case "Instant" -> {
                    Instant current = (Instant) value;
                    ps.setTime(index, Time.valueOf(LocalTime.from(current.atZone(ZoneId.systemDefault()))));
                }
                case "LocalDateTime" -> ps.setTimestamp(index, Timestamp.valueOf((LocalDateTime) value));
                case "ZonedDateTime" -> {
                    ZonedDateTime current = (ZonedDateTime) value;
                    ps.setTimestamp(index, Timestamp.valueOf(current.toLocalDateTime()), Calendar.getInstance(TimeZone.getTimeZone(current.getZone())));
                }
                case "OffsetDateTime" -> {
                    OffsetDateTime current = (OffsetDateTime) value;
                    ps.setTimestamp(index, Timestamp.valueOf(current.toLocalDateTime()), Calendar.getInstance(TimeZone.getTimeZone(current.toZonedDateTime().getZone())));
                }
                case "Boolean", "boolean" -> ps.setBoolean(index, (Boolean) value);
                case "[B", "byte[]" -> ps.setBytes(index, (byte[]) value);
                default -> {
                    if (Blob.class.isAssignableFrom(cls)) {
                        Blob blob = connection.createBlob();
                        if (value instanceof byte[]) {
                            blob.setBytes(1, (byte[]) value);
                        }
                        ps.setBlob(index, blob);
                    } else if (Clob.class.isAssignableFrom(cls)) {
                        Clob clob = connection.createClob();
                        clob.setString(1, (String) value);
                        ps.setClob(index, clob);
                    } else if (NClob.class.isAssignableFrom(cls)) {
                        NClob nclob = connection.createNClob();
                        nclob.setString(1, (String) value);
                        ps.setNClob(index, nclob);
                    }
                }
            }
            return ps;
        } catch (SQLException e) {
            throw addPreparedStatementException(parameterType, index, value, e);
        }
    }
}
