package io.kestra.plugin.jdbc.duckdb;

import io.kestra.plugin.jdbc.AbstractCellConverter;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneId;

public class DuckDbCellConverter extends AbstractCellConverter {
    public DuckDbCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @SneakyThrows
    @Override
    public Object convertCell(int columnIndex, ResultSet rs, Connection connection) throws SQLException {
        Object data = rs.getObject(columnIndex);

        if (data == null) {
            return null;
        }

        if (data instanceof OffsetDateTime) {
            return data;
        }

        if (data instanceof Byte) {
            Byte col = (Byte) data;
            return col.intValue();
        }

        return super.convert(columnIndex, rs);
    }
}
