package io.kestra.plugin.jdbc.duckdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import lombok.SneakyThrows;
import org.duckdb.DuckDBArray;
import org.duckdb.DuckDBStruct;

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

        if (data instanceof OffsetDateTime odt) {
            return odt.withOffsetSameInstant(ZoneOffset.UTC);
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

        // Handle STRUCT type
        if (data instanceof DuckDBStruct struct) {
            return processDuckDbStruct(struct, columnIndex, rs);
        }

        // Also check column type name for STRUCT (in case instanceof check doesn't work)
        if (columnTypeName != null && columnTypeName.toUpperCase().startsWith("STRUCT")) {
            try {
                String structString = rs.getString(columnIndex);
                if (structString != null) {
                    return parseStructString(structString);
                }
            } catch (SQLException e) {
                // If getString fails, continue to default handling
            }
        }

        // Handle JSON type - get as string and parse
        if (columnTypeName != null && columnTypeName.equalsIgnoreCase("JSON")) {
            String jsonString = null;
            try {
                jsonString = rs.getString(columnIndex);
                if (jsonString == null) {
                    return null;
                }
                return JacksonMapper.toMap(jsonString);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Invalid data type [" + columnTypeName + "] with value [" + jsonString + "]", e);
            }
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
                } else if (objectArray[i] instanceof DuckDBStruct nestedStruct) {
                    resultArray[i] = processDuckDbStruct(nestedStruct, 0, null);
                } else {
                    resultArray[i] = objectArray[i];
                }
            }
            return resultArray;
        }
        return array;
    }

    private Object processDuckDbStruct(DuckDBStruct struct, int columnIndex, ResultSet rs) throws SQLException {
        // First try to get as string from ResultSet (most reliable if supported)
        if (rs != null && columnIndex > 0) {
            try {
                String structString = rs.getString(columnIndex);
                if (structString != null) {
                    return parseStructString(structString);
                }
            } catch (SQLException e) {
                // If getString is not supported or fails, fall back to toString() method
            }
        }
        
        // Fallback: use toString() method on the DuckDBStruct object
        return parseStructString(struct.toString());
    }

    private Object parseStructString(String structString) throws SQLException {
        if (structString == null) {
            return null;
        }
        
        try {
            // DuckDB struct format uses {'key': value} syntax
            // Try to parse it directly first (DuckDB might serialize as valid JSON in some cases)
            return JacksonMapper.toMap(structString);
        } catch (JsonProcessingException e) {
            // If that fails, try converting DuckDB struct format to JSON
            // Replace single quotes with double quotes for simple cases
            String jsonString = structString.replace('\'', '"');
            try {
                return JacksonMapper.toMap(jsonString);
            } catch (JsonProcessingException e2) {
                // If parsing still fails, return the string representation as fallback
                return structString;
            }
        }
    }
}
