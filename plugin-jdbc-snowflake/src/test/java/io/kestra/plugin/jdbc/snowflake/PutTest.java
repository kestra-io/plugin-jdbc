package io.kestra.plugin.jdbc.snowflake;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.*;

@KestraTest
class PutTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void putResultIsReturned() throws Exception {
        URI source = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/file/storage/put-test.csv"),
            new ByteArrayInputStream(
                "name\nTest\n".getBytes(StandardCharsets.UTF_8)
            )
        );

        RunContext runContext = runContextFactory.of(Map.of());

        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);

        when(connection.createStatement()).thenReturn(statement);
        when(statement.execute(anyString())).thenReturn(true);
        when(statement.getResultSet()).thenReturn(resultSet);

        when(resultSet.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnLabel(1)).thenReturn("FILE");
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getObject(1)).thenReturn("file.csv");

        Put put = spy(
            Put.builder()
                .from(Property.ofValue(source.toString()))
                .stageName(Property.ofValue("@MY_STAGE"))
                .build()
        );

        doReturn(connection).when(put).connection(runContext);

        Put.Output output = put.run(runContext);

        assertThat(
            output.getRows(),
            is(List.of(Map.of("FILE", "file.csv")))
        );

        verify(statement).execute(argThat(sql ->
            sql.startsWith("PUT 'file://")
                && sql.endsWith("' @MY_STAGE")
        ));
    }

@Test
void putWithNativeOptions() throws Exception {
    URI source = storageInterface.put(
        TenantService.MAIN_TENANT,
        null,
        URI.create("/file/storage/put-options-test.csv"),
        new ByteArrayInputStream(
            "name\nTest\n".getBytes(StandardCharsets.UTF_8)
        )
    );

    RunContext runContext = runContextFactory.of(Map.of());

    Connection connection = mock(Connection.class);
    Statement statement = mock(Statement.class);

    when(connection.createStatement()).thenReturn(statement);
    when(statement.execute(anyString())).thenReturn(false);

    Put put = spy(
        Put.builder()
            .from(Property.ofValue(source.toString()))
            .stageName(Property.ofValue("@MY_STAGE"))
            .autoCompress(Property.ofValue(true))
            .sourceCompression(Property.ofValue(Put.SourceCompression.GZIP))
            .overwrite(Property.ofValue(true))
            .parallel(Property.ofValue(4))
            .build()
    );

    doReturn(connection).when(put).connection(runContext);

    put.run(runContext);

    verify(statement).execute(argThat(sql ->
        sql.startsWith("PUT 'file://")
            && sql.contains(" @MY_STAGE")
            && sql.contains(" AUTO_COMPRESS = true")
            && sql.contains(" SOURCE_COMPRESSION = GZIP")
            && sql.contains(" OVERWRITE = true")
            && sql.contains(" PARALLEL = 4")
    ));
}
}