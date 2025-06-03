package io.kestra.plugin.jdbc.clickhouse;

import com.clickhouse.data.value.UnsignedLong;
import io.kestra.plugin.jdbc.AbstractCellConverter;

import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.regex.Pattern;

public class ClickHouseCellConverter extends AbstractCellConverter {
    private static final Pattern PATTERN = Pattern.compile("DateTime(64)?\\((.*)'(.*)'\\)");
    private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]");

    public ClickHouseCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @Override
    public Object convertCell(int columnIndex, ResultSet rs, Connection connection) throws SQLException {
        Object data = rs.getObject(columnIndex);

        if (data == null) {
            return null;
        }

        Object columnVal = rs.getObject(columnIndex);
        String columnTypeName = rs.getMetaData().getColumnTypeName(columnIndex);

        if (columnTypeName.equals("Array(Int8)")) {
            Object raw = rs.getObject(columnIndex);
            if (raw instanceof java.sql.Array sqlArray) {
                Object arrayObj = sqlArray.getArray();
                if (arrayObj instanceof Object[] objArray) {
                    Byte[] byteArray = new Byte[objArray.length];
                    for (int i = 0; i < objArray.length; i++) {
                        byteArray[i] = ((Number) objArray[i]).byteValue();
                    }
                    return byteArray;
                }
            }
        }

        if (columnTypeName.startsWith("Array(")) {
            Object value = rs.getObject(columnIndex);

            if (value instanceof java.sql.Array) {
                return ((java.sql.Array) value).getArray();
            }
            return value;
        }

        if (columnTypeName.equals("DateTime")) {
            return rs.getTimestamp(columnIndex).toLocalDateTime();
        } else if (columnTypeName.startsWith("DateTime(") || columnTypeName.startsWith("DateTime64(")) {
            return rs.getObject(columnIndex, ZonedDateTime.class);
        }

        if (columnTypeName.equals("Int8")) {
            return columnVal;
        }

        if (columnTypeName.equals("Date")) {
            if (columnVal instanceof java.sql.Date) {
                return ((java.sql.Date) columnVal).toLocalDate();
            } else if (columnVal instanceof LocalDate) {
                return columnVal;
            } else {
                return LocalDate.parse(columnVal.toString());
            }
        }

        if (columnTypeName.startsWith("Tuple(")) {
            if (columnVal instanceof Object[]) {
                return Arrays.asList((Object[]) columnVal);
            }
            return columnVal;
        }

        if (columnTypeName.equals("IPv4")) {
            Inet4Address col = (Inet4Address) columnVal;
            return col.toString().substring(1);
        }

        if (columnTypeName.equals("IPv6")) {
            Inet6Address col = (Inet6Address) columnVal;
            return col.toString().substring(1);
        }

        if (columnTypeName.equals("UInt64")) {
            return switch (columnVal) {
                case UnsignedLong unsigned -> unsigned.longValue();
                case BigInteger bigInt -> bigInt.longValue();
                case Number number -> number.longValue();
                default -> throw new IllegalArgumentException("Unexpected type for UInt64: " + columnVal.getClass());
            };
        }

        if (columnTypeName.equals("UInt16")) {
            if (columnVal instanceof Number) {
                return ((Number) columnVal).intValue();
            } else {
                throw new IllegalArgumentException("Unexpected type for UInt16: " + columnVal.getClass());
            }
        }

        if ("Float32".equals(columnTypeName)) {
            return rs.getFloat(columnIndex);
        }

        if ("Float64".equals(columnTypeName)) {
            return rs.getDouble(columnIndex);
        }

        return super.convert(columnIndex, rs);
    }
}
