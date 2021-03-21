package io.kestra.plugin.jdbc.vertica;

import com.vertica.jdbc.VerticaDayTimeInterval;
import com.vertica.jdbc.VerticaYearMonthInterval;
import io.kestra.plugin.jdbc.AbstractCellConverter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneId;

public class VerticaCellConverter extends AbstractCellConverter {
    public VerticaCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @Override
    public Object convertCell(int columnIndex, ResultSet rs) throws SQLException {
        Object data = rs.getObject(columnIndex);

        if (data == null) {
            return null;
        }

        String columnTypeName = rs.getMetaData().getColumnTypeName(columnIndex);
        Object columnVal = rs.getObject(columnIndex);

        switch (columnTypeName.toLowerCase()) {
            case "time":
                return rs.getTime(columnIndex);
            case "timetz":
                return OffsetTime.ofInstant(rs.getTimestamp(columnIndex).toInstant(), zoneId);
            case "timestamptz":
            case "timestamp with time zone":
                return rs.getTimestamp(columnIndex).toInstant().atZone(zoneId);
            case "datetime":
            case "timestamp":
                return rs.getTimestamp(columnIndex).toLocalDateTime();
            case "interval day to second":
                if (columnVal instanceof VerticaDayTimeInterval) {
                    VerticaDayTimeInterval interval = (VerticaDayTimeInterval) columnVal;

                    return Duration
                        .parse(
                            "P" + interval.getDay() + "DT" +
                                interval.getHour() + "H" +
                                interval.getMinute() + "M" +
                                interval.getSecond() + "S"
                        )
                        .plusNanos(interval.getFraction());
                } else {
                    throw new IllegalArgumentException("Illegal type '" + columnVal.getClass() + "' for interval day to second");
                }

            case "interval year to month":
                if (columnVal instanceof VerticaYearMonthInterval) {
                    VerticaYearMonthInterval interval = (VerticaYearMonthInterval) columnVal;
                    return Period.of(interval.getYear(), interval.getMonth(), 0);
                } else {
                    throw new IllegalArgumentException("Illegal type '" + columnVal.getClass() + "' for interval year to month");
                }
        }

        return super.convert(columnIndex, rs);
    }
}
