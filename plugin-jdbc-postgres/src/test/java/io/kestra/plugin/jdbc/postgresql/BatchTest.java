package io.kestra.plugin.jdbc.postgresql;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.*;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
public class BatchTest extends AbstractRdbmsTest {
    @Test
    void insert() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 0; i < 5; i++) {
            FileSerde.write(output, Arrays.asList(
                i,
                true,
                "four",
                "Here is a varchar",
                "Here is a text column data",
                null,
                32767,
                9223372036854775807L,
                9223372036854776000F,
                9223372036854776000d,
                9223372036854776000d,
                new BigDecimal("2147483645.1234"),
                LocalDate.parse("2030-12-25"),
                LocalTime.parse("04:05:30"),
                OffsetTime.parse("13:05:06+01:00"),
                LocalDateTime.parse("2004-10-19T10:23:54.999999"),
                ZonedDateTime.parse("2004-10-19T08:23:54.250+02:00[Europe/Paris]"),
                "P10Y4M5DT0H0M10S",
                new int[]{100, 200, 300},
                new String[][]{new String[]{"meeting", "lunch"}, new String[]{"training", "presentation"}},
                "{\"color\":\"red\",\"value\":\"#f00\"}",
                ImmutableMap.of(
                    "amount", new BigDecimal("149.9"),
                    "currency", "EUR"
                ),
                Hex.decodeHex("DEADBEEF".toCharArray()),
                "a quick brown fox jumped over the lazy dog"
            ));
        }

        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Batch task = Batch.builder()
            .url(Property.ofValue(getUrl()))
            .url(Property.ofValue(TestUtils.url()))
            .username(Property.ofValue(TestUtils.username()))
            .password(Property.ofValue(TestUtils.password()))
            .ssl(Property.ofValue(TestUtils.ssl()))
            .sslMode(Property.ofValue(TestUtils.sslMode()))
            .sslRootCert(Property.ofValue(TestUtils.ca()))
            .sslCert(Property.ofValue(TestUtils.cert()))
            .sslKey(Property.ofValue(TestUtils.key()))
            .sslKeyPassword(Property.ofValue(TestUtils.keyPass()))
            .from(Property.ofValue(uri.toString()))
            .sql(Property.ofValue("insert into pgsql_types\n" +
                "(\n" +
                "  concert_id,\n" +
                "  available,\n" +
                "  a,\n" +
                "  b,\n" +
                "  c,\n" +
                "  d,\n" +
                "  play_time,\n" +
                "  library_record,\n" +
                "  floatn_test,\n" +
                "  double_test,\n" +
                "  real_test,\n" +
                "  numeric_test,\n" +
                "  date_type,\n" +
                "  time_type,\n" +
                "  timez_type,\n" +
                "  timestamp_type,\n" +
                "  timestampz_type,\n" +
                "  interval_type,\n"+
                "  pay_by_quarter,\n" +
                "  schedule,\n" +
                "  json_type,\n" +
                "  jsonb_type,\n" +
                "  blob_type,\n" +
                "  tsvector_col" +
                ")\n" +
                "values\n" +
                "(\n " +
                "  ?,\n" +
                "  ?,\n" +
                "  ?,\n" +
                "  ?,\n" +
                "  ?,\n" +
                "  ?,\n" +
                "  ?,\n" +
                "  ?,\n" +
                "  ?,\n" +
                "  ?,\n" +
                "  ?,\n" +
                "  ?,\n" +
                "  ?,\n" +
                "  ?,\n" +
                "  ?,\n" +
                "  ?,\n" +
                "  ?,\n" +
                "  ?,\n" +
                "  ?,\n" +
                "  ?,\n" +
                "  ?,\n" +
                "  ?::jsonb,\n" +
                "  ?,\n" +
                "  ?::tsvector\n" +
                ")"
            ))
            .build();

        AbstractJdbcBatch.Output runOutput = task.run(runContext);

        assertThat(runOutput.getRowCount(), is(5L));
    }

    @Test
    public void namedInsert() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 0; i < 5; i++) {
            FileSerde.write(output, ImmutableMap.builder()
                .put("id", 125)
                .put("name", "kestra")
                .put("address", "here")
                .build()
            );
        }

        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Batch task = Batch.builder()
            .url(Property.ofValue(TestUtils.url()))
            .username(Property.ofValue(TestUtils.username()))
            .password(Property.ofValue(TestUtils.password()))
            .ssl(Property.ofValue(TestUtils.ssl()))
            .sslMode(Property.ofValue(TestUtils.sslMode()))
            .sslRootCert(Property.ofValue(TestUtils.ca()))
            .sslCert(Property.ofValue(TestUtils.cert()))
            .sslKey(Property.ofValue(TestUtils.key()))
            .sslKeyPassword(Property.ofValue(TestUtils.keyPass()))
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

        for (int i = 0; i < 5; i++) {
            FileSerde.write(output, ImmutableMap.builder()
                .put("id", 125)
                .put("name", "kestra")
                .put("address", "here")
                .build()
            );
        }

        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Batch task = Batch.builder()
            .url(Property.ofValue(TestUtils.url()))
            .username(Property.ofValue(TestUtils.username()))
            .password(Property.ofValue(TestUtils.password()))
            .ssl(Property.ofValue(TestUtils.ssl()))
            .sslMode(Property.ofValue(TestUtils.sslMode()))
            .sslRootCert(Property.ofValue(TestUtils.ca()))
            .sslCert(Property.ofValue(TestUtils.cert()))
            .sslKey(Property.ofValue(TestUtils.key()))
            .sslKeyPassword(Property.ofValue(TestUtils.keyPass()))
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
                32767,
                9223372036854775807L,
                9223372036854776000F,
                9223372036854776000d,
                9223372036854776000d,
                new BigDecimal("2147483645.1234"),
                LocalDate.parse("2030-12-25"),
                LocalTime.parse("04:05:30"),
                OffsetTime.parse("13:05:06+01:00"),
                LocalDateTime.parse("2004-10-19T10:23:54.999999"),
                ZonedDateTime.parse("2004-10-19T08:23:54.250+02:00[Europe/Paris]"),
                "P10Y4M5DT0H0M10S",
                new int[]{100, 200, 300},
                new String[][]{new String[]{"meeting", "lunch"}, new String[]{"training", "presentation"}},
                "{\"color\":\"red\",\"value\":\"#f00\"}",
                Hex.decodeHex("DEADBEEF".toCharArray())
            ));
        }

        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Batch task = Batch.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .ssl(Property.ofValue(TestUtils.ssl()))
            .sslMode(Property.ofValue(TestUtils.sslMode()))
            .sslRootCert(Property.ofValue(TestUtils.ca()))
            .sslCert(Property.ofValue(TestUtils.cert()))
            .sslKey(Property.ofValue(TestUtils.key()))
            .sslKeyPassword(Property.ofValue(TestUtils.keyPass()))
            .from(Property.ofValue(uri.toString()))
            .table(Property.ofValue("pgsql_nosql"))
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
            .ssl(Property.ofValue(TestUtils.ssl()))
            .sslMode(Property.ofValue(TestUtils.sslMode()))
            .sslRootCert(Property.ofValue(TestUtils.ca()))
            .sslCert(Property.ofValue(TestUtils.cert()))
            .sslKey(Property.ofValue(TestUtils.key()))
            .sslKeyPassword(Property.ofValue(TestUtils.keyPass()))
            .from(Property.ofValue(uri.toString()))
            .table(Property.ofValue("namedInsert"))
            .columns(Property.ofValue(List.of("name")))
            .build();

        AbstractJdbcBatch.Output runOutput = task.run(runContext);

        assertThat(runOutput.getRowCount(), is(5L));
    }

    @Override
    protected String getUrl() {
        return TestUtils.url();
    }

    @Override
    protected String getUsername() {
        return TestUtils.username();
    }

    @Override
    protected String getPassword() {
        return TestUtils.password();
    }

    protected Connection getConnection() throws SQLException {
        Properties props = new Properties();
        props.put("jdbc.url", getUrl());
        props.put("user", getUsername());
        props.put("password", getPassword());

        try {
            PostgresService.handleSsl(props, runContextFactory.of(), new PgsqlTest.PostgresConnection());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return DriverManager.getConnection(props.getProperty("jdbc.url"), props);
    }


    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/postgres_insert.sql");
    }
}
