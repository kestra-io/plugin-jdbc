package io.kestra.plugin.jdbc.postgresql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.time.ZoneOffset;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.util.PGInterval;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PostgresIntervalCellConverterTest {
    private static Stream<Arguments> intervals() {
        return Stream.of(
            Arguments.of(new PGInterval(0, 0, 0, 0, 0, 10), "P0Y0M0DT0H0M10S"),
            Arguments.of(new PGInterval(0, 0, 0, 0, 0, 0.875), "P0Y0M0DT0H0M0.875S"),
            Arguments.of(new PGInterval(0, 0, 0, 0, 0, -0.125), "P0Y0M0DT0H0M-0.125S"),
            Arguments.of(new PGInterval(0, 0, 0, 0, 0, 0), "P0Y0M0DT0H0M0S"),
            Arguments.of(new PGInterval(100, 11, 456, 23, 59, 123456789.123456), "P100Y11M456DT23H59M123456789.123456S")
        );
    }

    @ParameterizedTest
    @MethodSource("intervals")
    void convertsIntervalWithoutLosingSeconds(PGInterval interval, String expected) throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(resultSet.getObject(1)).thenReturn(interval);
        when(resultSet.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnTypeName(1)).thenReturn("interval");

        Object actual = new PostgresCellConverter(ZoneOffset.UTC).convertCell(1, resultSet, null);

        assertEquals(expected, actual);
    }
}
