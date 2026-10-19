package io.kestra.plugin.jdbc.postgresql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.util.PGobject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PostgresJsonCellConverterTest {
    private final PostgresCellConverter converter = new PostgresCellConverter(ZoneId.of("UTC"));

    @ParameterizedTest
    @ValueSource(strings = {"json", "jsonb"})
    void convertsArrayValues(String type) throws Exception {
        assertThat(convert(type, "[1,2,3]"), equalTo(List.of(1, 2, 3)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"json", "jsonb"})
    void convertsScalarValues(String type) throws Exception {
        assertThat(convert(type, "42"), equalTo(42));
    }

    @Test
    void keepsObjectValuesAsMaps() throws Exception {
        assertThat(convert("jsonb", "{\"ok\":true}"), equalTo(Map.of("ok", true)));
    }

    @Test
    void convertsJsonNullToNull() throws Exception {
        assertNull(convert("jsonb", "null"));
    }

    @Test
    void convertsQuotedStringsWithoutRemovingQuotesFromContent() throws Exception {
        assertThat(convert("jsonb", "\"quoted value\""), equalTo("quoted value"));
    }

    @Test
    void convertsNestedArraysInObjects() throws Exception {
        assertThat(
            convert("jsonb", "{\"values\":[[1,2],[3,4]]}"),
            equalTo(Map.of("values", List.of(List.of(1, 2), List.of(3, 4))))
        );
    }

    private Object convert(String type, String value) throws Exception {
        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        PGobject json = new PGobject();
        json.setType(type);
        json.setValue(value);
        when(rs.getObject(anyInt())).thenReturn(json);
        when(rs.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnTypeName(anyInt())).thenReturn(type);
        return converter.convertCell(1, rs, mock(Connection.class));
    }
}
