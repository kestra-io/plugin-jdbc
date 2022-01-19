package io.kestra.plugin.jdbc.vertica;

import com.google.common.base.Strings;
import com.vertica.jdbc.VerticaDayTimeInterval;
import com.vertica.jdbc.VerticaYearMonthInterval;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;
import org.apache.commons.lang3.time.DurationFormatUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneId;
import java.util.Locale;

public class VerticaCellConverter extends AbstractCellConverter {
    public VerticaCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @Override
    public Object convertCell(int columnIndex, ResultSet rs, Connection connection) throws SQLException {
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

    @Override
    protected PreparedStatement addPreparedStatementValue(
        PreparedStatement ps,
        AbstractJdbcBatch.ParameterType parameterType,
        Object value,
        int index,
        Connection connection
    ) throws Exception {
        String type = parameterType.getTypeName(index).toLowerCase(Locale.ROOT);

        if (type.equals("interval day to second")) {
            Duration duration = this.parseDuration(value);

            if (duration != null) {
                ps.setString(index, DurationFormatUtils.formatDuration(
                    duration.toMillis(),
                    "dd HH:mm:SS",
                    true
                ));
                return ps;
            }
        } else if (type.equals("interval year to month")) {
            if (value instanceof String) {
                Period period = Period.parse((String) value);

                ps.setString(
                    index,
                    Strings.padStart(String.valueOf(period.getMonths()), 2, '0') + "-" +
                        Strings.padStart(String.valueOf(period.getDays()), 2, '0')
                );
                return ps;
            }
        }

        return super.addPreparedStatementValue(ps, parameterType, value, index, connection);
    }
}
