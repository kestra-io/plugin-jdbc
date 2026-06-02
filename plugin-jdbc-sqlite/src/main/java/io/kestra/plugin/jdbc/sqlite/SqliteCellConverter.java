package io.kestra.plugin.jdbc.sqlite;

import io.kestra.plugin.jdbc.AbstractCellConverter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.TimeZone;

public class SqliteCellConverter extends AbstractCellConverter {

    public SqliteCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @Override
    public Object convertCell(int columnIndex, ResultSet resultSet, Connection connection) throws SQLException {
        Object data = resultSet.getObject(columnIndex);

        if (data == null) {
            return null;
        }

        String columnTypeName = resultSet.getMetaData().getColumnTypeName(columnIndex);

        return switch (columnTypeName.toLowerCase()) {
            case "date" -> LocalDate.parse(resultSet.getString(columnIndex));
            case "datetime", "timestamp" -> {
                var calendar = Calendar.getInstance(TimeZone.getTimeZone(zoneId));
                var timestamp = resultSet.getTimestamp(columnIndex, calendar);
                yield timestamp != null ? timestamp.toInstant() : null;
            }
            case "time" -> LocalTime.parse(resultSet.getString(columnIndex));
            default -> super.convert(columnIndex, resultSet);
        };
    }
}
