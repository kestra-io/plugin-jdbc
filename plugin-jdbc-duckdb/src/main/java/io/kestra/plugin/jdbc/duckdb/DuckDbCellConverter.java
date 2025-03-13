package io.kestra.plugin.jdbc.duckdb;

import io.kestra.plugin.jdbc.AbstractCellConverter;
import lombok.SneakyThrows;
import org.duckdb.DuckDBArray;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

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

        String columnTypeName = rs.getMetaData().getColumnTypeName(columnIndex);

        if (data instanceof OffsetDateTime) {
            return data;
        }

        if (columnTypeName.equalsIgnoreCase("TIMESTAMP")) {
            return rs.getTimestamp(columnIndex).toLocalDateTime().atZone(zoneId);
        }

        if (columnTypeName.equalsIgnoreCase("TIMESTAMPTZ")) {
            return rs.getTimestamp(columnIndex).toLocalDateTime().atOffset(ZoneOffset.UTC);
        }

        if (data instanceof Byte col) {
            return col.intValue();
        }

        if (data instanceof DuckDBArray array) {
            return processDuckDbArray(array);
        }

        return super.convert(columnIndex, rs);
    }

    private Object processDuckDbArray(DuckDBArray duckDBArray) throws SQLException {
        Object array = duckDBArray.getArray();
        if (array instanceof Object[] objectArray) {
            Object[] resultArray = new Object[objectArray.length];
            for (int i = 0; i < resultArray.length; i++) {
                if (objectArray[i] instanceof DuckDBArray nestedDuckDbArray) {
                    resultArray[i] = processDuckDbArray(nestedDuckDbArray);
                } else {
                    resultArray[i] = objectArray[i];
                }
            }
            return resultArray;
        }
        return array;
    }
}
