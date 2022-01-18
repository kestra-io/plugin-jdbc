package io.kestra.plugin.jdbc.postgresql;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@MicronautTest
public class BatchTest extends AbstractRdbmsTest {
    @Inject
    private ApplicationContext applicationContext;

    @Test
    void insert() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 0; i < 5; i++) {
            FileSerde.write(output, List.of(
                i,
                true,
                "four",
                "This is a varchar",
                "This is a text column data",
                32767,
                9223372036854775807L,
                9223372036854776000F,
                9223372036854776000d,
                9223372036854776000d,
                new BigDecimal("2147483645.1234"),
                LocalDate.parse("2030-12-25"),
                LocalTime.parse("04:05:30"),
                LocalTime.parse("13:05:06"),
                LocalDateTime.parse("2004-10-19T10:23:54.999999"),
                ZonedDateTime.parse("2004-10-19T08:23:54.250+02:00[Europe/Paris]"),
                "P10Y4M5DT0H0M10S",
                new int[]{100, 200, 300},
                new String[][]{new String[]{"meeting", "lunch"}, new String[]{"training", "presentation"}},
                "{\"color\":\"red\",\"value\":\"#f00\"}",
                Hex.decodeHex("DEADBEEF".toCharArray())
            ));
        }

        URI uri = storageInterface.put(URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Batch task = Batch.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .from(uri.toString())
            .sql("insert into pgsql_types values( ? , ? , ? , ? , ? , ? , ? , ? , ?, ? , ? , ? , ? , ? , ? , ?, ? , ? , ? , ? , ? )")
            .build();

        AbstractJdbcBatch.Output runOutput = task.run(runContext);
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


        URI uri = storageInterface.put(URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        ArrayList<String> columns = new ArrayList<>();
        columns.add("id");
        columns.add("name");
        columns.add("address");

        Batch task = Batch.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .from(uri.toString())
            .sql("insert into namedInsert values( ? , ? , ? )")
            .build();

        AbstractJdbcBatch.Output runOutput = task.run(runContext);
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


        URI uri = storageInterface.put(URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        ArrayList<String> columns = new ArrayList<>();
        columns.add("id");
        columns.add("name");

        Batch task = Batch.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .from(uri.toString())
            .sql("insert into namedInsert(id,name) values( ? , ? )")
            .columns(columns)
            .build();

        AbstractJdbcBatch.Output runOutput = task.run(runContext);
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
            PostgresService.handleSsl(props, new RunContext(applicationContext, Map.of()), new PgsqlTest.PostgresConnection());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return DriverManager.getConnection(props.getProperty("jdbc.url"), props);
    }

    public static class PostgresConnection implements PostgresConnectionInterface {

        @Override
        public Boolean getSsl() {
            return TestUtils.ssl();
        }

        @Override
        public SslMode getSslMode() {
            return TestUtils.sslMode();
        }

        @Override
        public String getSslRootCert() {
            return TestUtils.ca();
        }

        @Override
        public String getSslCert() {
            return TestUtils.cert();
        }

        @Override
        public String getSslKey() {
            return TestUtils.key();
        }

        @Override
        public String getSslKeyPassword() {
            return TestUtils.keyPass();
        }
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/postgres_insert.sql");
    }
}