package io.kestra.plugin.jdbc.clickhouse;

import io.kestra.plugin.jdbc.AbstractCellConverter;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ClickHouseCellConverter extends AbstractCellConverter {
    private static final Pattern PATTERN = Pattern.compile("DateTime(64)?\\((.*)'(.*)'\\)");

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

        if (columnTypeName.startsWith("DateTime")) {
            Matcher matcher = PATTERN.matcher(columnTypeName);
            if (!matcher.find() || matcher.groupCount() < 3) {
                throw new IllegalArgumentException("Invalid Column Type '" + columnTypeName + "'");
            }

            return LocalDateTime
                .parse(
                    rs.getString(columnIndex),
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]")
                )
                .atZone(ZoneId.of(matcher.group(3)))
                .withZoneSameInstant(zoneId);
        }

        if (columnTypeName.equals("Int8")) {
            Byte col = (Byte) columnVal;
            return col.intValue();
        }

        if (columnTypeName.equals("Date")) {
            return columnVal;
        }

        if (columnTypeName.startsWith("Array(")) {
            return columnVal;
        }

        if (columnTypeName.startsWith("Tuple(")) {
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

        return super.convert(columnIndex, rs);
    }
}
