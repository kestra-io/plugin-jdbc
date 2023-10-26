package io.kestra.plugin.jdbc.arrowflight;

import io.kestra.plugin.jdbc.AbstractCellConverter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;

public class ArrowFlightCellConverter extends AbstractCellConverter {

    public ArrowFlightCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @Override
    public Object convertCell(int columnIndex, ResultSet resultSet, Connection connection) throws SQLException {
        return super.convert(columnIndex, resultSet);
    }
}
