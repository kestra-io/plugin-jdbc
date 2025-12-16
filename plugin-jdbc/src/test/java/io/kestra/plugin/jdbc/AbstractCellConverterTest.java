package io.kestra.plugin.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AbstractCellConverterTest {

    static class TestConverter extends AbstractCellConverter {
        TestConverter() {
            super(ZoneId.of("UTC"));
        }

        @Override
        public Object convertCell(int columnIndex, ResultSet rs, Connection connection) {
            return null;
        }
    }

    @Test
    void shouldParseIso8601String() {
        TestConverter converter = new TestConverter();

        Object parsed = converter.parseIsoTimestamp(
            "2025-02-01T07:15:30Z"
        );

        assertTrue(parsed instanceof OffsetDateTime);
        assertEquals(OffsetDateTime.parse("2025-02-01T07:15:30Z"), parsed);
    }

    @Test
    void shouldParseIsoOffsetString() {
        TestConverter converter = new TestConverter();

        Object parsed = converter.parseIsoTimestamp(
            "2025-02-01T07:15:30+01:00"
        );

        assertTrue(parsed instanceof OffsetDateTime);
        assertEquals(OffsetDateTime.parse("2025-02-01T07:15:30+01:00"), parsed);
    }

    @Test
    void shouldParseIsoZonedDateTimeString() {
        TestConverter converter = new TestConverter();

        Object parsed = converter.parseIsoTimestamp(
            "2025-02-01T07:15:30Z[Etc/UTC]"
        );

        assertTrue(parsed instanceof java.time.ZonedDateTime);
        assertEquals(ZonedDateTime.parse("2025-02-01T07:15:30Z[Etc/UTC]"), parsed);
    }


    @Test
    void shouldLeaveNonIsoStringUntouched() {
        TestConverter converter = new TestConverter();

        Object parsed = converter.parseIsoTimestamp("not-a-date");

        assertEquals("not-a-date", parsed);
    }
}
