package io.kestra.plugin.jdbc.access;

import io.kestra.plugin.jdbc.AbstractCellConverter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;

public class AccessCellConverter extends AbstractCellConverter {

    public AccessCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @Override
    public Object convertCell(int columnIndex, ResultSet resultSet, Connection connection) throws SQLException {
        Object data = resultSet.getObject(columnIndex);

        if (data == null) {
            return null;
        }

        // UCanAccess maps Access types to standard JDBC types; delegate to base converter
        return super.convert(columnIndex, resultSet);
    }
}
