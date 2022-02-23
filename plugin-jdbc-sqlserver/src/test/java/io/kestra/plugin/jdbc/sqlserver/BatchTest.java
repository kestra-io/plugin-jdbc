package io.kestra.plugin.jdbc.sqlserver;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.*;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@MicronautTest
public class BatchTest extends AbstractRdbmsTest {
    @Test
    void insert() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 0; i < 5; i++) {
            FileSerde.write(
                output,
                List.of(
                    9223372036854775807L,
                    2147483647,
                    32767,
                    255,
                    12345.12345D,
                    12345.12345F,
                    BigDecimal.valueOf(123.46),
                    BigDecimal.valueOf(12345.12345),
                    true,
                    BigDecimal.valueOf(3148.2929),
                    BigDecimal.valueOf(3148.1234),
                    "test      ",
                    "test",
                    "test      ",
                    "test",
                    "test",
                    "test",
                    LocalTime.parse("12:35:29"),
                    LocalDate.parse("2007-05-08"),
                    ZonedDateTime.parse("2007-05-08T12:35+02:00[Europe/Paris]"),
                    ZonedDateTime.parse("2007-05-08T12:35:29.123+02:00[Europe/Paris]"),
                    ZonedDateTime.parse("2007-05-08T12:35:29.123456700+02:00[Europe/Paris]"),
                    OffsetDateTime.parse("2007-05-08T12:35:29.123456700+12:15")
                )
            );
        }

        URI uri = storageInterface.put(URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Batch task = Batch.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .from(uri.toString())
            .sql("insert into sqlserver_types values( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )")
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
                .put("t_id", i)
                .put("t_name", "Kestra")
                .put("t_address", "here").build()
            );
        }

        URI uri = storageInterface.put(URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Batch task = Batch.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .from(uri.toString())
            .sql("insert into namedInsert values( ? , ? , ? )")
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
                .put("t_id",i)
                .put("t_name","Kestra")
                .build()
            );
        }


        URI uri = storageInterface.put(URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        ArrayList<String> columns = new ArrayList<>();
        columns.add("t_id");
        columns.add("t_name");

        Batch task = Batch.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .from(uri.toString())
            .sql("insert into namedInsert (t_id,t_name) values( ? , ? )")
            .columns(columns)
            .build();

        AbstractJdbcBatch.Output runOutput = task.run(runContext);

        assertThat(runOutput.getRowCount(), is(5L));
    }

    @Override
    protected String getUrl() {
        return "jdbc:sqlserver://localhost:41433;trustServerCertificate=true";
    }

    @Override
    protected String getUsername() {
        return "sa";
    }

    @Override
    protected String getPassword() {
        return "Sqls3rv3r_Pa55word!";
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        try {
            RunScript.execute(getConnection(), new StringReader("DROP TABLE sqlserver_types;DROP TABLE namedInsert;"));
        } catch (Exception ignored) {
        }
        executeSqlScript("scripts/sqlserver_insert.sql");
    }
}
