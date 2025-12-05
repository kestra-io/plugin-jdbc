package io.kestra.plugin.jdbc.postgresql;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;
import org.postgresql.jdbc.PgArray;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;
import org.postgresql.util.HStoreConverter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Map;

public class PostgresCellConverter extends AbstractCellConverter {
    public PostgresCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @Override
    public Object convertCell(int columnIndex, ResultSet rs, Connection connection) throws SQLException {

        Object data = rs.getObject(columnIndex);

        if (data == null) {
            return null;
        }

        String pgColumnType = rs.getMetaData().getColumnTypeName(columnIndex);

        switch (pgColumnType.toLowerCase()) {
            case "time":
                return LocalTime.parse(rs.getTime(columnIndex).toString());
            case "timetz":
            case "time with time zone":
                // FIXME : Since Time uses 01-01-1970 by default, timezone needs to be adjusted
                return LocalTime.from(rs.getTimestamp(columnIndex).toInstant().atZone(zoneId));
            case "timestamp":
                return rs.getTimestamp(columnIndex).toLocalDateTime();
            case "timestamptz":
            case "timestamp with time zone":
                return rs.getTimestamp(columnIndex).toInstant().atZone(zoneId);
            case "interval":
                PGInterval interval = (PGInterval) data;
                // Returns an iso 8601 duration format
                return getISO8601Interval(interval.getYears(), interval.getMonths(), interval.getDays(), interval.getHours(), interval.getMinutes(), (int) interval.getSeconds());
            case "hstore":
                // Convert hstore to a Map<String, String>
                Map<String, String> hstoreMap = HStoreConverter.fromString(rs.getString(columnIndex));
                return hstoreMap;
        }

        Class<?> clazz = data.getClass();

        // PgArray
        if (clazz.equals(PgArray.class)) {
            return ((PgArray) data).getArray();
        }

        // Pgobject (used for json and custom/composite pgsql type ...)
        if (clazz.equals(PGobject.class)) {
            PGobject o = ((PGobject) data);
            String type = o.getType();
            switch (type.toLowerCase()) {
                case "tsvector":
                {
                    return o.getValue();
                }
                case "json":
                case "jsonb":
                    try {
                        return JacksonMapper.toMap(o.getValue());
                    } catch (JsonProcessingException e) {
                        throw new IllegalArgumentException("Invalid data type [" + type + "] with value [" + o.getValue() + "]");
                    }
                case "void":
                    return null;
                default:
                    throw new IllegalArgumentException("PGobject of type [" + type + "] is not supported");
            }
        }

        return super.convert(columnIndex, rs);
    }

    private String getISO8601Interval(int years, int months, int days, int hours, int minutes, int seconds) {
        return "P" + years + "Y" + months + "M" + days + "DT" + hours + "H" + minutes + "M" + seconds + "S";
    }

    @Override
    public PreparedStatement addPreparedStatementValue(PreparedStatement ps, AbstractJdbcBatch.ParameterType parameterType, Object value, int index, Connection connection) throws Exception {
        Class<?> cls = parameterType.getClass(index);
        String typeName = parameterType.getTypeName(index);

        if (cls == PGInterval.class || "interval".equalsIgnoreCase(typeName)) {
            Duration duration = this.parseDuration(value);

            if (duration != null) {
                ps.setObject(index, new PGInterval(duration.toString()));
                return ps;
            } if (value instanceof String) {
                ps.setObject(index, new PGInterval((String) value));
                return ps;
            }
        }

        if ("json".equalsIgnoreCase(typeName) || "jsonb".equalsIgnoreCase(typeName)) {
            PGobject jsonObject = new PGobject();
            jsonObject.setType(typeName.toLowerCase());

            if (value instanceof String string) {
                jsonObject.setValue(string);
            } else {
                try {
                    jsonObject.setValue(JacksonMapper.ofJson().writeValueAsString(value));
                } catch (JsonProcessingException e) {
                    throw new IllegalArgumentException(
                        "Invalid JSON value for type '" + typeName + "' at index " + index,
                        e
                    );
                }
            }

            ps.setObject(index, jsonObject);
            return ps;
        }

        return super.addPreparedStatementValue(ps, parameterType, value, index, connection);
    }
}
