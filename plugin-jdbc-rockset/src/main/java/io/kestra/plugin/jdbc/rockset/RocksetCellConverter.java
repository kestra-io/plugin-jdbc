package io.kestra.plugin.jdbc.rockset;

import com.rockset.jdbc.RocksetArrayOverride;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import lombok.SneakyThrows;
import rockset.com.fasterxml.jackson.core.type.TypeReference;
import rockset.com.fasterxml.jackson.databind.ObjectMapper;
import rockset.com.fasterxml.jackson.databind.node.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Map;

public class RocksetCellConverter extends AbstractCellConverter {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> TYPE_REFERENCE = new TypeReference<>() {};

    public RocksetCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @SneakyThrows
    @Override
    public Object convertCell(int columnIndex, ResultSet rs, Connection connection) throws SQLException {
        String columnTypeName = rs.getMetaData().getColumnTypeName(columnIndex);

        Object data = rs.getObject(columnIndex);

        if (data == null) {
            return null;
        }

        if (data instanceof TextNode) {
            return ((TextNode) data).asText();
        }

        if (data instanceof BooleanNode) {
            return ((BooleanNode) data).asBoolean();
        }

        if (data instanceof DoubleNode) {
            return ((DoubleNode) data).asDouble();
        }

        if (data instanceof ObjectNode) {
            return MAPPER.convertValue(data, TYPE_REFERENCE);
        }

        if (data instanceof NullNode) {
            return null;
        }

        if (columnTypeName.equals("array")) {
            return RocksetArrayOverride.getArray(data);
        }

        return super.convert(columnIndex, rs);
    }
}
