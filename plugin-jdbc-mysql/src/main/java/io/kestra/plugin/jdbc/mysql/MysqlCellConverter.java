package io.kestra.plugin.jdbc.mysql;

import com.mysql.cj.jdbc.result.ResultSetImpl;
import io.kestra.plugin.jdbc.AbstractCellConverter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;

public class MysqlCellConverter extends AbstractCellConverter {
    public MysqlCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @Override
    public Object convertCell(int columnIndex, ResultSet rs, Connection connection) throws SQLException {
        Object data = rs.getObject(columnIndex);

        if (data == null) {
            return null;
        }

        String columnTypeName = rs.getMetaData().getColumnTypeName(columnIndex);

        switch (columnTypeName.toLowerCase()) {
            case "time":
                return ((ResultSetImpl) rs).getLocalTime(columnIndex);
            case "datetime":
            case "timestamp":
                return rs.getTimestamp(columnIndex).toInstant();
        }

        return super.convert(columnIndex, rs);
    }
}
