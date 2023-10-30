package io.kestra.plugin.jdbc.sqlite;

import io.kestra.plugin.jdbc.AbstractCellConverter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;

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
            case "datetime", "timestamp" -> resultSet.getTimestamp(columnIndex).toInstant();
            case "time" -> LocalTime.parse(resultSet.getString(columnIndex));
            default -> super.convert(columnIndex, resultSet);
        };
    }
}
