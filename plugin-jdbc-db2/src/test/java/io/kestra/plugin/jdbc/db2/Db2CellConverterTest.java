package io.kestra.plugin.jdbc.db2;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLXML;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class Db2CellConverterTest {

    private Db2CellConverter converter() {
        return new Db2CellConverter(ZoneId.of("UTC"));
    }

    private ResultSet mockResultSet(String columnTypeName, Object data) throws Exception {
        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnTypeName(anyInt())).thenReturn(columnTypeName);
        when(rs.getObject(anyInt())).thenReturn(data);
        return rs;
    }

    @Test
    void testBlob_isMaterializedBeforeResultSetCanClose() throws Exception {
        byte[] expected = "some binary content".getBytes();
        Blob blob = mock(Blob.class);
        when(blob.getBinaryStream()).thenReturn(new ByteArrayInputStream(expected));

        ResultSet rs = mockResultSet("blob", blob);
        when(rs.getBlob(anyInt())).thenReturn(blob);

        Object result = converter().convertCell(1, rs, mock(Connection.class));

        // The converted value must be the actual bytes, not the live Blob locator itself --
        // a caller reading this after the row/connection is gone must not need the Blob anymore.
        assertArrayEquals(expected, (byte[]) result);
        // The locator is released once its content has been read.
        verify(blob, times(1)).free();
    }

    @Test
    void testClob_isMaterializedBeforeResultSetCanClose() throws Exception {
        String expected = "some clob text content";
        Clob clob = mock(Clob.class);
        when(clob.getCharacterStream()).thenReturn(new StringReader(expected));

        ResultSet rs = mockResultSet("clob", clob);
        when(rs.getClob(anyInt())).thenReturn(clob);

        Object result = converter().convertCell(1, rs, mock(Connection.class));

        assertEquals(expected, result);
        verify(clob, times(1)).free();
    }

    @Test
    void testNClob_isMaterializedBeforeResultSetCanClose() throws Exception {
        String expected = "some nclob text content";
        NClob nclob = mock(NClob.class);
        when(nclob.getCharacterStream()).thenReturn(new StringReader(expected));

        ResultSet rs = mockResultSet("nclob", nclob);
        when(rs.getNClob(anyInt())).thenReturn(nclob);

        Object result = converter().convertCell(1, rs, mock(Connection.class));

        assertEquals(expected, result);
        verify(nclob, times(1)).free();
    }

    @Test
    void testSqlXml_isMaterializedBeforeResultSetCanClose() throws Exception {
        String expected = "<root><child>value</child></root>";
        SQLXML sqlxml = mock(SQLXML.class);
        when(sqlxml.getString()).thenReturn(expected);

        ResultSet rs = mockResultSet("xml", sqlxml);
        when(rs.getSQLXML(anyInt())).thenReturn(sqlxml);

        Object result = converter().convertCell(1, rs, mock(Connection.class));

        assertEquals(expected, result);
        verify(sqlxml, times(1)).free();
    }

    @Test
    void testBlob_nullValueReturnsNull() throws Exception {
        ResultSet rs = mockResultSet("blob", null);

        Object result = converter().convertCell(1, rs, mock(Connection.class));

        assertEquals(null, result);
        // getBlob() should never even be called once getObject() already reported no row data.
        Mockito.verify(rs, Mockito.never()).getBlob(anyInt());
    }
}
