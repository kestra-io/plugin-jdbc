package io.kestra.plugin.jdbc.sqlite;

import io.kestra.plugin.jdbc.AbstractCellConverter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.TimeZone;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;

public class SqliteCellConverter extends AbstractCellConverter {

    private static final DateTimeFormatter SQLITE_DATETIME_FORMATTER =
    new DateTimeFormatterBuilder()
        .appendOptional(DateTimeFormatter.ISO_ZONED_DATE_TIME)
        .appendOptional(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        .appendOptional(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        .appendOptional(new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .toFormatter())
        .appendOptional(DateTimeFormatter.ISO_LOCAL_DATE)
        .toFormatter();

    public SqliteCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @Override
    public Object convertCell(int columnIndex, ResultSet resultSet, Connection connection) throws SQLException {
        Object data = resultSet.getObject(columnIndex);

        if (data == null) {
            return null;
        }

        String columnTypeName = resultSet.getMetaData().getColumnTypeName(columnIndex);

        return switch (columnTypeName.toLowerCase()) {
            case "date" -> LocalDate.parse(resultSet.getString(columnIndex));
            case "datetime", "timestamp" -> {
                if (data instanceof String text) {
                    yield parseTextToInstant(text);
                }
                var calendar = Calendar.getInstance(TimeZone.getTimeZone(zoneId));
                var timestamp = resultSet.getTimestamp(columnIndex, calendar);
                yield timestamp != null ? timestamp.toInstant() : null;
            }
            case "time" -> LocalTime.parse(resultSet.getString(columnIndex));
            default -> super.convert(columnIndex, resultSet);
        };
    }
    private Instant parseTextToInstant(String value) {
    TemporalAccessor temporal = SQLITE_DATETIME_FORMATTER.parseBest(
        value,
        ZonedDateTime::from,
        LocalDateTime::from,
        LocalDate::from
    );
    return switch (temporal) {
        case ZonedDateTime zdt -> zdt.toInstant();
        case LocalDateTime ldt -> ldt.atZone(zoneId).toInstant();
        case LocalDate ld      -> ld.atStartOfDay(zoneId).toInstant();
        default -> throw new IllegalArgumentException(
            "Unsupported SQLite datetime format: " + value
        );
    };
    }
}
