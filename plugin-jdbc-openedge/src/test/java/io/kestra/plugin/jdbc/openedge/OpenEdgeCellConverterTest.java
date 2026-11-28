package io.kestra.plugin.jdbc.openedge;

import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OpenEdgeCellConverterTest {
    private final OpenEdgeCellConverter converter = new OpenEdgeCellConverter(ZoneId.of("UTC"));

    @Test
    void convertsTimeToLocalTime() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        LocalTime expected = LocalTime.of(14, 25, 36);

        when(resultSet.getObject(1)).thenReturn(Time.valueOf(expected));
        when(resultSet.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnTypeName(1)).thenReturn("TIME");
        when(resultSet.getTime(1)).thenReturn(Time.valueOf(expected));

        assertEquals(expected, converter.convertCell(1, resultSet, null));
    }

    @Test
    void convertsOpenEdgeTimestampToLocalDateTime() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        LocalDateTime expected = LocalDateTime.of(2026, 11, 28, 14, 25, 36, 123_000_000);

        when(resultSet.getObject(1)).thenReturn(Timestamp.valueOf(expected));
        when(resultSet.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnTypeName(1)).thenReturn("TIMESTAMP");
        when(resultSet.getTimestamp(1)).thenReturn(Timestamp.valueOf(expected));

        assertEquals(expected, converter.convertCell(1, resultSet, null));
    }

    @Test
    void convertsAblDateTimeAliasToLocalDateTime() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        LocalDateTime expected = LocalDateTime.of(2026, 11, 28, 14, 25, 36);

        when(resultSet.getObject(1)).thenReturn(Timestamp.valueOf(expected));
        when(resultSet.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnTypeName(1)).thenReturn("DATETIME");
        when(resultSet.getTimestamp(1)).thenReturn(Timestamp.valueOf(expected));

        assertEquals(expected, converter.convertCell(1, resultSet, null));
    }

    @Test
    void preservesNullValues() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getObject(1)).thenReturn(null);

        assertNull(converter.convertCell(1, resultSet, null));
    }

    @Test
    void delegatesDateAndOtherSupportedTypesToSharedConverter() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        LocalDate expected = LocalDate.of(2026, 11, 28);

        when(resultSet.getObject(1)).thenReturn(Date.valueOf(expected));
        when(resultSet.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnTypeName(1)).thenReturn("DATE");

        assertEquals(expected, converter.convertCell(1, resultSet, null));
    }
}
