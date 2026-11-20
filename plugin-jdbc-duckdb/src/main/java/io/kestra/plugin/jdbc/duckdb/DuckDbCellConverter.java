package io.kestra.plugin.jdbc.duckdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import lombok.SneakyThrows;
import org.duckdb.DuckDBArray;
import org.duckdb.DuckDBStruct;
import org.duckdb.JsonNode;

import java.math.BigInteger;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

public class DuckDbCellConverter extends AbstractCellConverter {
    private static final Pattern HOUR_ONLY_OFFSET = Pattern.compile("[+-]\\d{2}$");

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

        if ("VARIANT".equalsIgnoreCase(columnTypeName)) {
            return processVariant(data);
        }

        if ("TIMESTAMPTZ_NS".equalsIgnoreCase(columnTypeName)) {
            // Parse the 2.0 driver's TIMESTAMPTZ_NS text without losing nanoseconds.
            var value = rs.getString(columnIndex);
            if ("infinity".equals(value) || "-infinity".equals(value)) {
                return value;
            }
            var iso = value.replace(' ', 'T');
            if (HOUR_ONLY_OFFSET.matcher(iso).find()) {
                iso += ":00";
            }
            return OffsetDateTime.parse(iso).withOffsetSameInstant(ZoneOffset.UTC);
        }

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
                return JacksonMapper.toObject(jsonString);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Invalid data type [" + columnTypeName + "] with value [" + jsonString + "]", e);
            }
        }

        return super.convert(columnIndex, rs);
    }

    // Materialize native VARIANT containers before closing the result set.
    private Object processVariant(Object value) throws SQLException, JsonProcessingException {
        if (value instanceof DuckDBStruct struct) {
            return processVariant(struct.getMap());
        }
        if (value instanceof Map<?, ?> map) {
            var result = new LinkedHashMap<String, Object>();
            for (var entry : map.entrySet()) {
                result.put(entry.getKey().toString(), processVariant(entry.getValue()));
            }
            return result;
        }
        if (value instanceof Array array) {
            var elements = (Object[]) array.getArray();
            var result = new ArrayList<>(elements.length);
            for (var element : elements) {
                result.add(processVariant(element));
            }
            return result;
        }
        if (value instanceof JsonNode json) {
            return JacksonMapper.toObject(json.toString());
        }
        if (value instanceof Blob blob) {
            return readBlob(blob);
        }
        if (value instanceof Byte number) {
            return number.intValue();
        }
        if (value instanceof BigInteger integer) {
            // Keep JSON-derived HUGEINT values numeric, narrowing only when lossless.
            if (integer.bitLength() < 32) {
                return integer.intValue();
            }
            if (integer.bitLength() < 64) {
                return integer.longValue();
            }
            return integer;
        }
        if (value instanceof UUID) {
            return value.toString();
        }
        if (value instanceof OffsetDateTime dateTime) {
            return dateTime.withOffsetSameInstant(ZoneOffset.UTC);
        }
        if (value instanceof Timestamp timestamp) {
            return timestamp.toLocalDateTime().atZone(zoneId);
        }
        return value;
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
