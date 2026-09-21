package io.kestra.plugin.jdbc;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    void testStringValueToTimestamp_JdbcFormatWithFractionalSeconds() throws Exception {
        // java.sql.Timestamp.toString() emits "yyyy-MM-dd HH:mm:ss.f[fffffffff]" (1 to 9 digits)
        verifyTimestampConversion(
            "2019-10-30 12:00:00.1",
            Timestamp.valueOf(LocalDateTime.of(2019, 10, 30, 12, 0, 0, 100_000_000))
        );
        verifyTimestampConversion(
            "2019-10-30 12:00:00.12",
            Timestamp.valueOf(LocalDateTime.of(2019, 10, 30, 12, 0, 0, 120_000_000))
        );
        verifyTimestampConversion(
            "2019-10-30 12:00:00.123",
            Timestamp.valueOf(LocalDateTime.of(2019, 10, 30, 12, 0, 0, 123_000_000))
        );
        verifyTimestampConversion(
            "2019-10-30 12:00:00.123456789",
            Timestamp.valueOf(LocalDateTime.of(2019, 10, 30, 12, 0, 0, 123_456_789))
        );
    }

    @Test
    void testStringValueToTimestamp_UnparseableSurfacesActionableError() {
        // No candidate format matches: a >9-digit fraction overflows the JDBC formatter, and the
        // value has neither an ISO 'T' nor a bare-date shape. The failure must surface as the
        // wrapped, actionable message from addPreparedStatementException rather than a raw
        // DateTimeParseException.
        AbstractCellConverter converter = createConverter();
        PreparedStatement ps = Mockito.mock(PreparedStatement.class);
        AbstractJdbcBatch.ParameterType parameterType = Mockito.mock(AbstractJdbcBatch.ParameterType.class);

        int columnIndex = 1;
        Mockito.when(parameterType.getClass(columnIndex)).thenReturn((Class) java.sql.Timestamp.class);
        Mockito.when(parameterType.getTypeName(columnIndex)).thenReturn("TIMESTAMP");

        String badValue = "2019-10-30 12:00:00.1234567890";

        Exception exception = assertThrows(
            Exception.class,
            () -> converter.addPreparedStatementValue(ps, parameterType, badValue, columnIndex, null)
        );

        assertTrue(exception.getMessage().contains("Unable to transform data"));
        assertTrue(exception.getMessage().contains(badValue));

        Throwable cause = exception.getCause();
        assertNotNull(cause);
        assertInstanceOf(IllegalArgumentException.class, cause);
        assertTrue(cause.getMessage().contains("Unsupported date format"));
    }

    @Test
    void testStringValueToTime_JdbcFormat() throws Exception {
        verifyTimeConversion(
            "2019-10-30 12:00:00",
            Timestamp.valueOf(LocalDateTime.of(2019, 10, 30, 12, 0))
        );
    }

    @Test
    void testReadBlob_returnsContentAndFrees() throws Exception {
        byte[] expected = "some binary content".getBytes();
        Blob blob = mock(Blob.class);
        when(blob.getBinaryStream()).thenReturn(new ByteArrayInputStream(expected));

        assertArrayEquals(expected, AbstractCellConverter.readBlob(blob));
        verify(blob, times(1)).free();
    }

    @Test
    void testReadClob_returnsContentAndFrees() throws Exception {
        String expected = "some clob text content";
        Clob clob = mock(Clob.class);
        when(clob.getCharacterStream()).thenReturn(new StringReader(expected));

        assertEquals(expected, AbstractCellConverter.readClob(clob));
        verify(clob, times(1)).free();
    }

    @Test
    void testReadNClob_returnsContentAndFrees() throws Exception {
        String expected = "some nclob text content";
        NClob nclob = mock(NClob.class);
        when(nclob.getCharacterStream()).thenReturn(new StringReader(expected));

        assertEquals(expected, AbstractCellConverter.readNClob(nclob));
        verify(nclob, times(1)).free();
    }

    @Test
    void testReadSqlXml_returnsContentAndFrees() throws Exception {
        String expected = "<root><child>value</child></root>";
        SQLXML sqlxml = mock(SQLXML.class);
        when(sqlxml.getString()).thenReturn(expected);

        assertEquals(expected, AbstractCellConverter.readSqlXml(sqlxml));
        verify(sqlxml, times(1)).free();
    }

    @Test
    void testReadBlob_freeFailureDoesNotDiscardAlreadyReadData() throws Exception {
        // Regression for the bug flagged in review on #1005/#1014: free() throwing in the
        // finally block must not replace the value the try block already successfully returned.
        byte[] expected = "some binary content".getBytes();
        Blob blob = mock(Blob.class);
        when(blob.getBinaryStream()).thenReturn(new ByteArrayInputStream(expected));
        doThrow(new SQLException("connection already closed")).when(blob).free();

        assertArrayEquals(expected, AbstractCellConverter.readBlob(blob));
    }

    @Test
    void testReadClob_freeFailureDoesNotDiscardAlreadyReadData() throws Exception {
        String expected = "some clob text content";
        Clob clob = mock(Clob.class);
        when(clob.getCharacterStream()).thenReturn(new StringReader(expected));
        doThrow(new SQLException("connection already closed")).when(clob).free();

        assertEquals(expected, AbstractCellConverter.readClob(clob));
    }

    @Test
    void testReadNClob_freeFailureDoesNotDiscardAlreadyReadData() throws Exception {
        String expected = "some nclob text content";
        NClob nclob = mock(NClob.class);
        when(nclob.getCharacterStream()).thenReturn(new StringReader(expected));
        doThrow(new SQLException("connection already closed")).when(nclob).free();

        assertEquals(expected, AbstractCellConverter.readNClob(nclob));
    }

    @Test
    void testReadSqlXml_freeFailureDoesNotDiscardAlreadyReadData() throws Exception {
        String expected = "<root><child>value</child></root>";
        SQLXML sqlxml = mock(SQLXML.class);
        when(sqlxml.getString()).thenReturn(expected);
        doThrow(new SQLException("connection already closed")).when(sqlxml).free();

        assertEquals(expected, AbstractCellConverter.readSqlXml(sqlxml));
    }

    @Test
    void testReadBlob_readFailureSurfacesEvenWhenFreeAlsoFails() throws Exception {
        // The other half of the same bug: if the read itself fails, that real error must
        // propagate -- not get silently replaced by a failure from the free() cleanup.
        Blob blob = mock(Blob.class);
        InputStream brokenStream = mock(InputStream.class);
        when(brokenStream.read(Mockito.any(byte[].class))).thenThrow(new IOException("connection reset"));
        when(blob.getBinaryStream()).thenReturn(brokenStream);
        doThrow(new SQLException("connection already closed")).when(blob).free();

        SQLException exception = assertThrows(SQLException.class, () -> AbstractCellConverter.readBlob(blob));
        assertEquals("Error reading BLOB data", exception.getMessage());
        assertInstanceOf(IOException.class, exception.getCause());
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