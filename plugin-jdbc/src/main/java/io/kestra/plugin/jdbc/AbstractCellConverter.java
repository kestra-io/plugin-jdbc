package io.kestra.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringEscapeUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;

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

    public abstract PreparedStatement adaptedStatement(PreparedStatement ps, Object prop, int index, Connection connection) throws Exception;

    protected PreparedStatement adaptStatement(PreparedStatement ps, Object prop, int index, Connection connection) throws Exception {
        if (prop instanceof Integer) {
            ps.setInt(index, (Integer) prop);
            return ps;
        } else if (prop instanceof String) {
            ps.setString(index, (String) prop);
            return ps;
        } else if (prop instanceof UUID) {
            ps.setObject(index, prop);
            return ps;
        } else if (prop instanceof Long) {
            ps.setLong(index, (Long) prop);
            return ps;
        } else if (prop instanceof BigInteger) {
            ps.setLong(index, ((BigInteger) prop).longValue());
            return ps;
        } else if (prop instanceof Float) {
            ps.setFloat(index, (Float) prop);
            return ps;
        } else if (prop instanceof BigDecimal) {
            ps.setBigDecimal(index, (BigDecimal) prop);
            return ps;
        } else if (prop instanceof Double) {
            ps.setDouble(index, (Double) prop);
            return ps;
        } else if (prop instanceof LocalDateTime) {
            ps.setTimestamp(index, Timestamp.valueOf((LocalDateTime) prop));
            return ps;
        } else if (prop instanceof LocalDate) {
            ps.setDate(index, Date.valueOf((LocalDate) prop));
            return ps;
        } else if (prop instanceof OffsetTime) {
            ps.setTime(index,
                Time.valueOf( ((OffsetTime) prop).toLocalTime()),
                Calendar.getInstance());
            return ps;
        } else if (prop instanceof LocalTime) {
            ps.setTime(index, Time.valueOf((LocalTime) prop));
            return ps;
        } else if (prop instanceof ZonedDateTime) {
            ps.setTimestamp(index, Timestamp.valueOf((((ZonedDateTime) prop)).toLocalDateTime()), Calendar.getInstance());
            return ps;
        } else if (prop instanceof OffsetDateTime) {
            ps.setTimestamp(index, Timestamp.valueOf((((OffsetDateTime) prop)).toLocalDateTime()), Calendar.getInstance());
            return ps;
        } else if (prop instanceof Instant) {
            ps.setTimestamp(index, Timestamp.valueOf(LocalDateTime.ofInstant((Instant) prop, ZoneOffset.UTC)));
            return ps;
        } else if (prop instanceof Boolean) {
            ps.setBoolean(index, (Boolean) prop);
            return ps;
        } else if (prop instanceof Byte){
            ps.setByte(index, (Byte) prop);
            return ps;
        } else if (prop instanceof byte[]){
            ps.setBytes(index, (byte[]) prop);
            return ps;
        } else if (prop instanceof ArrayList) {
            ps.setArray(index, connection.createArrayOf("Object", ((ArrayList) prop).toArray()) ) ;
            return ps;
        }
        throw new Exception();
    }
}
