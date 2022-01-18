package io.kestra.plugin.jdbc.clickhouse;

import io.kestra.plugin.jdbc.AbstractCellConverter;
import ru.yandex.clickhouse.ClickHouseArray;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

        if (columnVal instanceof ClickHouseArray) {
            ClickHouseArray col = (ClickHouseArray) columnVal;
            return col.getArray();
        }

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

        return super.convert(columnIndex, rs);
    }

    public PreparedStatement adaptedStatement(PreparedStatement ps, Object prop, int index, Connection connection) throws Exception {
        return this.adaptStatement(ps, prop, index, connection);
    }
}
