package io.kestra.plugin.jdbc;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class AbstractCellConverterTest {

    private AbstractCellConverter createConverter() {
        // We use UTC zone for the converter
        return new AbstractCellConverter(ZoneId.of("UTC")) {
            @Override
            public Object convertCell(int columnIndex, ResultSet rs, Connection connection) throws SQLException {
                return null;
            }
        };
    }

    @Test
    void testStringValueToTimestamp_IsoLocal() throws Exception {
        verifyTimestampConversion(
            "2019-10-30T00:00:00",
            Timestamp.valueOf(LocalDateTime.of(2019, 10, 30, 0, 0))
        );
    }

    @Test
    void testStringValueToTimestamp_IsoZoned() throws Exception {
        ZonedDateTime utcTime =
            ZonedDateTime.of(2019, 10, 30, 15, 30, 0, 0, ZoneId.of("UTC"));

        verifyTimestampConversion(
            "2019-10-30T15:30:00Z",
            Timestamp.from(utcTime.toInstant())
        );
    }

    @Test
    void testStringValueToTimestamp_SimpleDate() throws Exception {
        verifyTimestampConversion(
            "2019-10-30",
            Timestamp.valueOf(LocalDateTime.of(2019, 10, 30, 0, 0))
        );
    }

    @Test
    void testStringValueToDateIsoZoned() throws Exception {
        ZonedDateTime utcTime =
            ZonedDateTime.of(2019, 10, 30, 15, 30, 0, 0, ZoneId.of("UTC"));

        verifyDateConversion(
            "2019-10-30T15:30:00Z",
            Timestamp.from(utcTime.toInstant())
        );
    }

    @Test
    void testStringValueToDate_IsoLocal() throws Exception {
        verifyDateConversion(
            "2019-10-30T00:00:00",
            Timestamp.valueOf(LocalDateTime.of(2019, 10, 30, 0, 0))
        );
    }

    @Test
    void testStringValueToDate_SimpleDate() throws Exception {
        verifyDateConversion(
            "2019-10-30",
            Timestamp.valueOf(LocalDateTime.of(2019, 10, 30, 0, 0))
        );
    }

    @Test
    void testStringValueToTimestamp_JdbcFormat() throws Exception {
        verifyTimestampConversion(
            "2019-10-30 12:00:00",
            Timestamp.valueOf(LocalDateTime.of(2019, 10, 30, 12, 0))
        );
    }

    @Test
    void testStringValueToTime_JdbcFormat() throws Exception {
        verifyTimeConversion(
            "2019-10-30 12:00:00",
            Timestamp.valueOf(LocalDateTime.of(2019, 10, 30, 12, 0))
        );
    }




    private void verifyTimestampConversion(String inputString, Timestamp expectedTimestamp) throws Exception {
        AbstractCellConverter converter = createConverter();
        PreparedStatement ps = Mockito.mock(PreparedStatement.class);
        AbstractJdbcBatch.ParameterType parameterType = Mockito.mock(AbstractJdbcBatch.ParameterType.class);

        int columnIndex = 1;
        Mockito.when(parameterType.getClass(columnIndex)).thenReturn((Class) java.sql.Timestamp.class);
        Mockito.when(parameterType.getTypeName(columnIndex)).thenReturn("TIMESTAMP");

        // Run method
        converter.addPreparedStatementValue(ps, parameterType, inputString, columnIndex, null);

        // Capture result
        ArgumentCaptor<Timestamp> argument = ArgumentCaptor.forClass(Timestamp.class);
        verify(ps, times(1)).setTimestamp(eq(columnIndex), argument.capture());

        // Assert
        // Timestamp.equals() checks the underlying milliseconds, ignoring TimeZone display differences
        assertEquals(expectedTimestamp, argument.getValue());
    }
    private void verifyDateConversion(String inputString, Timestamp expectedTimestamp) throws Exception {
        AbstractCellConverter converter = createConverter();
        PreparedStatement ps = Mockito.mock(PreparedStatement.class);
        AbstractJdbcBatch.ParameterType parameterType = Mockito.mock(AbstractJdbcBatch.ParameterType.class);

        int columnIndex = 1;
        Mockito.when(parameterType.getClass(columnIndex)).thenReturn((Class) java.sql.Date.class);
        Mockito.when(parameterType.getTypeName(columnIndex)).thenReturn("DATE");

        // Run method
        converter.addPreparedStatementValue(ps, parameterType, inputString, columnIndex, null);

        // Capture result
        ArgumentCaptor<Timestamp> argument = ArgumentCaptor.forClass(Timestamp.class);
        verify(ps, times(1)).setTimestamp(eq(columnIndex), argument.capture());

        // Assert
        // Timestamp.equals() checks the underlying milliseconds, ignoring TimeZone display differences
        assertEquals(expectedTimestamp, argument.getValue());
    }

    private void verifyTimeConversion(String inputString, Timestamp expectedTimestamp) throws Exception {
        AbstractCellConverter converter = createConverter();
        PreparedStatement ps = Mockito.mock(PreparedStatement.class);
        AbstractJdbcBatch.ParameterType parameterType = Mockito.mock(AbstractJdbcBatch.ParameterType.class);

        int columnIndex = 1;
        Mockito.when(parameterType.getClass(columnIndex)).thenReturn((Class) java.sql.Time.class);
        Mockito.when(parameterType.getTypeName(columnIndex)).thenReturn("TIME");

        // Run method
        converter.addPreparedStatementValue(ps, parameterType, inputString, columnIndex, null);

        // Capture result
        ArgumentCaptor<Timestamp> argument = ArgumentCaptor.forClass(Timestamp.class);
        verify(ps, times(1)).setTimestamp(eq(columnIndex), argument.capture());

        // Assert
        // Timestamp.equals() checks the underlying milliseconds, ignoring TimeZone display differences
        assertEquals(expectedTimestamp, argument.getValue());
    }
}