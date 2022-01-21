package io.kestra.plugin.jdbc;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.*;
import java.time.*;
import java.time.format.DateTimeParseException;
import java.util.*;

public abstract class AbstractCellConverter {
    protected ZoneId zoneId;

    public AbstractCellConverter(ZoneId zoneId) {
        this.zoneId = zoneId;
    }

    public abstract Object convertCell(int columnIndex, ResultSet rs, Connection connection) throws SQLException;

    private static final List<Class<?>> SIMPLE_TYPES = ImmutableList.of(
        java.lang.String.class,
        java.lang.Boolean.class,
        java.lang.Integer.class,
        java.lang.Short.class,
        java.lang.Long.class,
        java.lang.Float.class,
        java.lang.Double.class,
        java.math.BigDecimal.class,
        byte[].class
    );

    protected Object convert(int columnIndex, ResultSet rs) throws SQLException {
        Object data = rs.getObject(columnIndex);
        if (data == null) {
            return null;
        }

        final Class<?> clazz = data.getClass();

        // For "simple" types, we return the data as-is
        if (SIMPLE_TYPES.contains(clazz)) {
            return data;
        }

        if (clazz.equals(java.math.BigInteger.class)) {
            return data.toString();
        }

        if (clazz.equals(java.math.BigDecimal.class)) {
            return data.toString();
        }

        if (clazz.equals(java.sql.Date.class)) {
            return ((Date) data).toLocalDate();
        }

        if (clazz.equals(java.sql.Time.class)) {
            return ((java.sql.Time) data).toLocalTime();
        }

        if (clazz.equals(java.sql.Timestamp.class)) {
            return ((Timestamp) data).toInstant().atZone(zoneId);
        }

        if (clazz.equals(java.util.UUID.class)) {
            return data.toString();
        }

        String columnName = rs.getMetaData().getColumnName(columnIndex);

        throw new IllegalArgumentException("Data of type '" + clazz + "' for column '" + columnName + "' is not supported");
    }

    protected PreparedStatement addPreparedStatementValue(PreparedStatement ps, AbstractJdbcBatch.ParameterType parameterType, Object value, int index, Connection connection) throws Exception {
        Class<?> cls = parameterType.getClass(index);

        try {
            if (value == null) {
                ps.setNull(index, parameterType.getType(index));
                return ps;
            } else if (cls == Integer.class) {
                ps.setInt(index, (Integer) value);
                return ps;
            } else if (cls == String.class) {
                ps.setString(index, (String) value);
                return ps;
            } else if (cls == UUID.class) {
                ps.setObject(index, value);
                return ps;
            } else if (cls == Long.class) {
                if (value instanceof Integer) {
                    ps.setLong(index, ((Integer) value).longValue());
                    return ps;
                } else {
                    ps.setLong(index, (Long) value);
                    return ps;
                }
            } else if (cls == BigInteger.class) {
                ps.setLong(index, ((BigInteger) value).longValue());
                return ps;
            } else if (cls == Double.class) {
                ps.setDouble(index, (Double) value);
                return ps;
            } else if (cls == Float.class) {
                if (value instanceof Double) {
                    ps.setFloat(index, ((Double) value).floatValue());
                    return ps;
                } else {
                    ps.setFloat(index, (Float) value);
                    return ps;
                }
            } else if (cls == BigDecimal.class) {
                if (value instanceof Integer) {
                    ps.setBigDecimal(index, new BigDecimal((Integer) value));
                    return ps;
                } else {
                    ps.setBigDecimal(index, (BigDecimal) value);
                    return ps;
                }
            } else if (cls == java.sql.Date.class) {
                if (value instanceof LocalDate) {
                    ps.setDate(index, Date.valueOf((LocalDate) value));
                    return ps;
                }
                if (value instanceof LocalDateTime) {
                    ps.setDate(index, Date.valueOf(((LocalDateTime) value).toLocalDate()));
                    return ps;
                }
            } else if (cls == java.sql.Time.class) {
                if (value instanceof LocalTime) {
                    ps.setTime(index, Time.valueOf((LocalTime) value));
                    return ps;
                } else if (value instanceof OffsetTime) {
                    OffsetTime current = (OffsetTime) value;
                    ps.setTime(index, Time.valueOf(current.toLocalTime()), Calendar.getInstance(TimeZone.getTimeZone(current.getOffset())));
                    return ps;
                } else if (value instanceof Instant) {
                    Instant current = (Instant) value;
                    ps.setTime(index, Time.valueOf(LocalTime.from(current.atZone(this.zoneId))));
                    return ps;
                }
            } else if (cls == java.sql.Timestamp.class) {
                if (value instanceof LocalDateTime) {
                    ps.setTimestamp(index, Timestamp.valueOf((LocalDateTime) value));
                    return ps;
                } else if (value instanceof ZonedDateTime) {
                    ZonedDateTime current = ((ZonedDateTime) value);
                    ps.setTimestamp(index, Timestamp.valueOf(current.toLocalDateTime()), Calendar.getInstance(TimeZone.getTimeZone(current.getZone())));
                    return ps;
                } else if (value instanceof OffsetDateTime) {
                    OffsetDateTime current = ((OffsetDateTime) value);
                    ps.setTimestamp(index, Timestamp.valueOf(current.toLocalDateTime()), Calendar.getInstance(TimeZone.getTimeZone(current.toZonedDateTime().getZone())));
                    return ps;
                } else if (value instanceof Instant) {
                    ps.setTimestamp(index, Timestamp.valueOf(LocalDateTime.ofInstant((Instant) value, ZoneOffset.UTC)));
                    return ps;
                } else if (value instanceof LocalDate) {
                    ps.setTimestamp(index, Timestamp.valueOf(((LocalDate) value).atStartOfDay()));
                    return ps;
                }
            } else if (cls == Boolean.class) {
                ps.setBoolean(index, (Boolean) value);
                return ps;
            } else if (cls.getName().equals("[B")) {
                ps.setBytes(index, (byte[]) value);
                return ps;
            } else if (cls == java.sql.Array.class) {
                Collection<?> collection = ((Collection<?>) value);

                ps.setArray(index, connection.createArrayOf(
                    parameterType.getTypeName(index).substring(1),
                    collection.toArray())
                );

                return ps;
            } else if (Blob.class.isAssignableFrom(cls)) {
                if (value.getClass().getName().equals("[B")) {
                    Blob blob = connection.createBlob();
                    blob.setBytes(1, (byte[]) value);

                    ps.setBlob(index, blob);
                    return ps;
                }
            } else if (Clob.class.isAssignableFrom(cls)) {
                if (value instanceof String) {
                    Clob blob = connection.createClob();
                    blob.setString(1, (String) value);

                    ps.setClob(index, blob);
                    return ps;
                }
            } else if (NClob.class.isAssignableFrom(cls)) {
                if (value instanceof String) {
                    NClob blob = connection.createNClob();
                    blob.setString(1, (String) value);

                    ps.setClob(index, blob);
                    return ps;
                }
            }
        } catch (Exception e) {
            throw addPreparedStatementException(parameterType, index, value, e);
        }

        throw addPreparedStatementException(parameterType, index, value, null);
    }

    protected Duration parseDuration(Object value) {
        if (value instanceof Duration) {
            return (Duration) value;
        } else if (value instanceof String) {
            try {
                return Duration.parse((String) value);
            } catch (DateTimeParseException ignored) {

            }
        } else if (value instanceof BigDecimal) {
            BigDecimal current = (BigDecimal) value;
            return Duration.ofSeconds(
                current.longValue(),
                current.subtract(new BigDecimal(current.longValue())).multiply(new BigDecimal(1000000000L)).intValue()
            );
        }

        return null;
    }

    protected Exception addPreparedStatementException(AbstractJdbcBatch.ParameterType parameterType, int index, Object value, Throwable e) {
        return new Exception("Unable to transform data with " +
            "type '" + parameterType.getTypeName(index) + "', " +
            "class '" + parameterType.getClass(index) + "', " +
            "index '" + index + "', " +
            "value '" + (value != null ? value.toString() : "null") + "', " +
            "valueClass '" + (value != null ? value.getClass() : "null") + "'",
            e
        );
    }
}
