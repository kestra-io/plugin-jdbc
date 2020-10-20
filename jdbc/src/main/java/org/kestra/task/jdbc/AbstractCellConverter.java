package org.kestra.task.jdbc;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.List;

public abstract class AbstractCellConverter {

    protected ZoneId zoneId;

    public AbstractCellConverter(ZoneId zoneId) {
        this.zoneId = zoneId;
    }

    public abstract Object convertCell(int columnIndex, ResultSet rs) throws SQLException;

    private static final List<Class> SIMPLE_TYPES = ImmutableList.of(
        java.lang.String.class,
        java.lang.Boolean.class,
        java.lang.Integer.class,
        java.lang.Short.class,
        java.lang.Long.class,
        java.lang.Float.class,
        java.lang.Double.class,
        byte[].class
    );

    protected Object convert(int columnIndex, ResultSet rs) throws SQLException {

        Object data = rs.getObject(columnIndex);
        if (data == null) {
            return null;
        }

        final Class clazz = data.getClass();

        // For "simple" types, we return the data as-is
        if (SIMPLE_TYPES.contains(clazz)) {
            return data;
        }

        if (clazz.equals(java.math.BigInteger.class)) {
            return ((BigInteger) data).toString();
        }

        if (clazz.equals(java.math.BigDecimal.class)) {
            return ((BigDecimal) data).toString();
        }

        if (clazz.equals(java.sql.Date.class)) {
            return ((Date) data).toLocalDate();
        }

        throw new IllegalArgumentException("Data of type [" + clazz + "] is not supported");
    }


}
