package io.kestra.plugin.jdbc;

import com.google.common.collect.ImmutableList;

import java.sql.*;
import java.time.ZoneId;
import java.util.List;

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
        java.util.UUID.class,
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

        if (clazz.equals(java.sql.Timestamp.class)) {
            return ((Timestamp) data).toInstant().atZone(zoneId);
        }

        throw new IllegalArgumentException("Data of type [" + clazz + "] is not supported");
    }
}
