package io.kestra.plugin.jdbc.dremio;

import io.kestra.plugin.jdbc.AbstractCellConverter;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DremioCellConverter extends AbstractCellConverter {
    private static final Pattern PATTERN = Pattern.compile("DateTime(64)?\\((.*)'(.*)'\\)");
    private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]");

    public DremioCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @Override
    public Object convertCell(int columnIndex, ResultSet resultSet, Connection connection) throws SQLException {
        Object data = resultSet.getObject(columnIndex);

        if (data == null) {
            return null;
        }

        Object columnValue = resultSet.getObject(columnIndex);
        String columnTypeName = resultSet.getMetaData().getColumnTypeName(columnIndex);

        if (columnTypeName.equals("DateTime")) {
            // date with no TZ, we use the server default one
            return LocalDateTime
                .parse(
                    resultSet.getString(columnIndex),
                    DATE_TIME_FORMAT
                );
        } else if (columnTypeName.startsWith("DateTime")) {
            Matcher matcher = PATTERN.matcher(columnTypeName);
            if (!matcher.find() || matcher.groupCount() < 3) {
                throw new IllegalArgumentException("Invalid Column Type '" + columnTypeName + "'");
            }

            return LocalDateTime
                .parse(
                    resultSet.getString(columnIndex),
                    DATE_TIME_FORMAT
                )
                .atZone(ZoneId.of(matcher.group(3)))
                .withZoneSameInstant(zoneId);
        }

        if (columnTypeName.equals("Int8")) {
            Byte columnByte = (Byte) columnValue;
            return columnByte.intValue();
        }

        if (columnTypeName.equals("Date")) {
            return columnValue;
        }

        if (columnTypeName.startsWith("Array(")) {
            return columnValue;
        }

        if (columnTypeName.startsWith("Tuple(")) {
            return columnValue;
        }

        if (columnTypeName.equals("IPv4")) {
            Inet4Address col = (Inet4Address) columnValue;
            return col.toString().substring(1);
        }

        if (columnTypeName.equals("IPv6")) {
            Inet6Address col = (Inet6Address) columnValue;
            return col.toString().substring(1);
        }

        return super.convert(columnIndex, resultSet);
    }
}
