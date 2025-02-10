package io.kestra.plugin.jdbc.mariadb;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;

public class MariaDbCellConverter extends AbstractCellConverter {
    public MariaDbCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @Override
    public Object convertCell(int columnIndex, ResultSet rs, Connection connection) throws SQLException {
        Object data = rs.getObject(columnIndex);

        if (data == null) {
            return null;
        }

        String columnTypeName = rs.getMetaData().getColumnTypeName(columnIndex);

        return switch (columnTypeName.toLowerCase()) {
            case "blob" -> rs.getBytes(columnIndex);
            case "json" -> {
                try {
                    yield JacksonMapper.ofJson(false).readValue(rs.getString(columnIndex), Object.class);
                } catch (JsonProcessingException e) {
                    throw new SQLException(e);
                }
            }
            default -> super.convert(columnIndex, rs);
        };

    }

    @Override
    public PreparedStatement addPreparedStatementValue(PreparedStatement ps, AbstractJdbcBatch.ParameterType parameterType, Object value, int index, Connection connection) throws Exception {
        throw new UnsupportedOperationException();
    }
}
