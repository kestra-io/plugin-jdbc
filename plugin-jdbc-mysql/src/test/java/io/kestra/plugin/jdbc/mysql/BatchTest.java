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
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.time.*;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
public class BatchTest extends AbstractRdbmsTest {
    @Inject
    private ApplicationContext applicationContext;

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
            .url(Property.of(getUrl()))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .from(Property.of(uri.toString()))
            .chunk(Property.of(1))
            .sql(Property.of("""
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
            .url(Property.of(getUrl()))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .from(Property.of(uri.toString()))
            .sql(Property.of("insert into namedInsert values( ? , ? , ? )"))
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
            .url(Property.of(getUrl()))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .from(Property.of(uri.toString()))
            .sql(Property.of("insert into namedInsert(id,name) values( ? , ? )"))
            .columns(Property.of(Arrays.asList("id", "name")))
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
            .url(Property.of(getUrl()))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .from(Property.of(uri.toString()))
            .table(Property.of("mysql_types"))
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
            .url(Property.of(getUrl()))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .from(Property.of(uri.toString()))
            .table(Property.of("namedInsert"))
            .columns(Property.of(List.of("name")))
            .build();

        AbstractJdbcBatch.Output runOutput = task.run(runContext);

        assertThat(runOutput.getRowCount(), is(5L));
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
