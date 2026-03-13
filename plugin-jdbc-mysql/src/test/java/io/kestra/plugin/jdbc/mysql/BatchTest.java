package io.kestra.plugin.jdbc.mysql;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import io.micronaut.context.ApplicationContext;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotNull;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.time.*;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
public class BatchTest extends AbstractRdbmsTest {
    @Test
    void insert() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 1; i < 6; i++) {
            FileSerde.write(output, Arrays.asList(
                i,
                true,
                "four",
                "Here is a varchar",
                "Here is a text column data",
                null,
                -9223372036854775808L,
                1844674407370955161L,
                new byte[]{0b000101},
                9223372036854776000F,
                9223372036854776000D,
                2147483645.1234D,
                new BigDecimal("5.36"),
                new BigDecimal("999.99"),
                LocalDate.parse("2030-12-25"),
                "2050-12-31 22:59:57.150150",
                LocalTime.parse("04:05:30"),
                "2004-10-19T10:23:54.999999",
                "2025",
                "{\"color\": \"red\", \"value\": \"#f00\"}",
                Hex.decodeHex("DEADBEEF".toCharArray())
            ));
        }

        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Batch task = Batch.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .from(Property.ofValue(uri.toString()))
            .chunk(Property.ofValue(1))
            .sql(Property.ofValue("""
                 INSERT INTO mysql_types (
                    concert_id,
                    available,
                    a,
                    b,
                    c,
                    d,
                    play_time,
                    library_record,
                    bitn_test,
                    floatn_test,
                    double_test,
                    doublen_test,
                    numeric_test,
                    salary_decimal,
                    date_type,
                    datetime_type,
                    time_type,
                    timestamp_type,
                    year_type,
                    json_type,
                    blob_type
                ) VALUES (
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?
                );
                 """)
            )
            .build();

        AbstractJdbcBatch.Output runOutput = task.run(runContext);

        assertThat(runOutput.getRowCount(), is(5L));
    }

    @Test
    public void namedInsert() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 1; i < 6; i++) {
            FileSerde.write(output, ImmutableMap.builder()
                .put("id", i)
                .put("name", "kestra")
                .put("address", "here")
                .build()
            );
        }

        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Batch task = Batch.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .from(Property.ofValue(uri.toString()))
            .sql(Property.ofValue("insert into namedInsert values( ? , ? , ? )"))
            .build();

        AbstractJdbcBatch.Output runOutput = task.run(runContext);

        assertThat(runOutput.getRowCount(), is(5L));
    }

    @Test
    public void namedColumnsInsert() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 1; i < 6; i++) {
            FileSerde.write(output, ImmutableMap.builder()
                .put("id", i)
                .put("name", "kestra")
                .put("address", "here")
                .build()
            );
        }

        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Batch task = Batch.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .from(Property.ofValue(uri.toString()))
            .sql(Property.ofValue("insert into namedInsert(id,name) values( ? , ? )"))
            .columns(Property.ofValue(Arrays.asList("id", "name")))
            .build();

        AbstractJdbcBatch.Output runOutput = task.run(runContext);

        assertThat(runOutput.getRowCount(), is(5L));
    }

    @Test
    public void noSqlForInsert() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 1; i < 6; i++) {
            FileSerde.write(output, Arrays.asList(
                i,
                true,
                "four",
                "Here is a varchar",
                "Here is a text column data",
                null,
                -9223372036854775808L,
                1844674407370955161L,
                new byte[]{0b000101},
                9223372036854776000F,
                9223372036854776000D,
                2147483645.1234D,
                new BigDecimal("5.36"),
                new BigDecimal("999.99"),
                LocalDate.parse("2030-12-25"),
                "2050-12-31 22:59:57.150150",
                LocalTime.parse("04:05:30"),
                "2004-10-19T10:23:54.999999",
                "2025",
                "{\"color\": \"red\", \"value\": \"#f00\"}",
                Hex.decodeHex("DEADBEEF".toCharArray())
            ));
        }

        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Batch task = Batch.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .from(Property.ofValue(uri.toString()))
            .table(Property.ofValue("mysql_types"))
            .build();

        AbstractJdbcBatch.Output runOutput = task.run(runContext);

        assertThat(runOutput.getRowCount(), is(5L));
    }

    @Test
    public void noSqlWithNamedColumnsForInsert() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 1; i < 6; i++) {
            FileSerde.write(output, List.of(
	            "Mario"
            ));
        }

        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Batch task = Batch.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .from(Property.ofValue(uri.toString()))
            .table(Property.ofValue("namedInsert"))
            .columns(Property.ofValue(List.of("name")))
            .build();

        AbstractJdbcBatch.Output runOutput = task.run(runContext);

        assertThat(runOutput.getRowCount(), is(5L));
    }

    @Test
    void shouldWorkWithChunks() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = File.createTempFile("chunk_test_", ".trs");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            int base = (int) (System.currentTimeMillis() % 100000);

            for (int i = 0; i < 5; i++) {
                FileSerde.write(output, Arrays.asList(base + i));
            }
        }

        URI uri = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new FileInputStream(tempFile)
        );

        Batch task = Batch.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .from(Property.ofValue(uri.toString()))
            .table(Property.ofValue("namedInsert"))
            .sql(Property.ofValue("insert into namedInsert(id) values(?)"))
            .chunk(Property.ofValue(2))
            .build();

        AbstractJdbcBatch.Output output = task.run(runContext);

        assertThat(output.getRowCount(), is(5L));
    }

    @Test
    void localFailsIfFileTooLarge() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = File.createTempFile("local_fail_", ".trs");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            for (int i = 0; i < 100; i++) {
                FileSerde.write(output, Arrays.asList(i));
            }
        }

        URI uri = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new FileInputStream(tempFile)
        );

        Batch task = Batch.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .from(Property.ofValue(uri.toString()))
            .sql(Property.ofValue("insert into namedInsert(id) values(?)"))
            .inputHandling(Property.ofValue(AbstractJdbcBatch.InputHandling.LOCAL))
            .localBufferMaxBytes(Property.ofValue(1L))
            .build();

        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> task.run(runContext)
        );
    }

    @Test
    void shouldHandleLocalInputHandling() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = File.createTempFile("local_input_", ".trs");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            int base = (int) (System.currentTimeMillis() % 100000);

            for (int i = 0; i < 3; i++) {
                FileSerde.write(output, Arrays.asList(base + i));
            }
        }

        URI uri = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new FileInputStream(tempFile)
        );

        Batch task = Batch.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .from(Property.ofValue(uri.toString()))
            .sql(Property.ofValue("insert into namedInsert(id) values(?)"))
            .inputHandling(Property.ofValue(AbstractJdbcBatch.InputHandling.LOCAL))
            .localBufferMaxBytes(Property.ofValue(1024L))
            .build();

        AbstractJdbcBatch.Output output = task.run(runContext);

        assertThat(output.getRowCount(), is(3L));
    }

    @Test
    void shouldRetryOnInputErrorWhenScopeInput() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = File.createTempFile("retry_input_", ".trs");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            FileSerde.write(output, List.of(1));
        }

        URI uri = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new FileInputStream(tempFile)
        );

        class FailingBatch extends Batch {
            int attempts = 0;

            @Override
            public Output run(RunContext runContext) throws Exception {
                if (attempts++ == 0) {
                    throw new IOException("Simulated input failure");
                }
                return super.run(runContext);
            }
        }

        Batch task = Batch.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .from(Property.ofValue(uri.toString()))
            .sql(Property.ofValue("insert into namedInsert(id) values(?)"))
            .retryScope(Property.ofValue(AbstractJdbcBatch.RetryScope.INPUT))
            .build();

        AbstractJdbcBatch.Output output = task.run(runContext);

        assertThat(output.getRowCount(), is(1L));
    }

    @Test
    void shouldNotRetryDbErrorWhenScopeInput() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = File.createTempFile("retry_db_fail_", ".trs");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            FileSerde.write(output, List.of(1));
        }

        URI uri = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new FileInputStream(tempFile)
        );

        class FailingBatch extends Batch {
            @Override
            public Output run(RunContext runContext) throws Exception {
                throw new SQLTransientException("Simulated DB transient failure");
            }
        }

        Batch task = Batch.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .from(Property.ofValue(uri.toString()))
            .sql(Property.ofValue("insert into namedInsert(id) values(?)"))
            .retryScope(Property.ofValue(AbstractJdbcBatch.RetryScope.INPUT))
            .build();

        org.junit.jupiter.api.Assertions.assertThrows(
            SQLTransientException.class,
            () -> task.run(runContext)
        );
    }

    @Test
    void shouldRetryDbErrorWhenScopeAll() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = File.createTempFile("retry_db_all_", ".trs");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            FileSerde.write(output, List.of(1));
        }

        URI uri = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new FileInputStream(tempFile)
        );

        class FailingBatch extends Batch {
            int attempts = 0;

            @Override
            public Output run(RunContext runContext) throws Exception {
                if (attempts++ == 0) {
                    throw new SQLTransientException("Simulated DB transient failure");
                }
                return super.run(runContext);
            }
        }

        Batch task = Batch.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .from(Property.ofValue(uri.toString()))
            .sql(Property.ofValue("insert into namedInsert(id) values(?)"))
            .retryScope(Property.ofValue(AbstractJdbcBatch.RetryScope.ALL))
            .build();

        AbstractJdbcBatch.Output output = task.run(runContext);

        assertThat(output.getRowCount(), is(1L));
    }

    @Override
    protected String getUrl() {
        return "jdbc:mysql://127.0.0.1:64790/kestra";
    }

    @Override
    protected String getUsername() {
        return "root";
    }

    @Override
    protected String getPassword() {
        return "mysql_passwd";
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/mysql_insert.sql");
    }
}
