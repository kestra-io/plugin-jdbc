package org.kestra.task.jdbc.postgresql;

import org.kestra.task.jdbc.AbstractCellConverter;
import org.postgresql.jdbc.PgArray;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;
import java.time.ZoneId;

public class PostgresCellConverter extends AbstractCellConverter {
    public PostgresCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @Override
    public Object convertCell(int columnIndex, ResultSet rs) throws SQLException {

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
                case "json":
                    return o.getValue();
                default:
                    throw new IllegalArgumentException("PGobject of type [" + type + "] is not supported");
            }
        }

        return super.convert(columnIndex, rs);
    }

    private String getISO8601Interval(int years, int months, int days, int hours, int minutes, int seconds) {
        return "P" + years + "Y" + months + "M" + days + "DT" + hours + "H" + minutes + "M" + seconds + "S";
    }
}
