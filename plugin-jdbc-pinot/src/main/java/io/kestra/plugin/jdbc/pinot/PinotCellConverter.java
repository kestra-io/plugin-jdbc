package io.kestra.plugin.jdbc.pinot;

import io.kestra.plugin.jdbc.AbstractCellConverter;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;

public class PinotCellConverter extends AbstractCellConverter {
    public PinotCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @SneakyThrows
    @Override
    public Object convertCell(int columnIndex, ResultSet rs, Connection connection) throws SQLException {
        return super.convert(columnIndex, rs);
    }
}
