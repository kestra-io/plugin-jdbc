package io.kestra.plugin.jdbc.rockset;

import com.fasterxml.jackson.databind.node.*;
import com.rockset.jdbc.RocksetArrayOverride;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;

public class RocksetCellConverter extends AbstractCellConverter {
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
            return JacksonMapper.toMap(data);
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
