package io.kestra.plugin.jdbc.vectorwise;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.jdbc.AbstractJdbcCopyIn;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

@MicronautTest
public class CopyInTest extends AbstractRdbmsTest {
    @Value("jdbc:ingres://url/database")
    protected String url;

    @Value("ingres")
    protected String user;

    @Value("password")
    protected String password;

    @Test
    @Disabled
    void insert() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());


        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 0; i < 10; i++) {
            FileSerde.write(output, List.of(
                125,
                32000,
                2147483640,
                9007199254740991L,
                new BigDecimal("1.55"),
                1.12365125789541,
                1.2F,
//                new BigDecimal("100.50"), pas de money
                "y",
                "yoplait",
                "tes",
                "autret",
                LocalDateTime.parse("1900-10-04T22:23:00.000"),
                LocalDate.parse("2006-05-16"),
                LocalDate.parse("2006-05-16"),
                LocalTime.parse("04:05:30"),
                LocalTime.parse("05:05:30"),
                LocalTime.parse("05:05:30"),
                LocalDateTime.parse("2006-05-16T00:00:00.000"),
                ZonedDateTime.parse("2006-05-16T02:00:00.000+02:00[Europe/Paris]"),
                ZonedDateTime.parse("2006-05-16T02:00:00.000+02:00[Europe/Paris]"),
//                "2007-10", pas d'interval ?
//                "4 00:00:00.000000", pas d'interval ?
                true
            ));
        }

        URI uri = storageInterface.put(URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        CopyIn task = CopyIn.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .from(uri.toString())
            .sql("insert into ingres.abdt_test values( ? , ? , ?, ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? )")
            .build();

        AbstractJdbcCopyIn.Output runOutput = task.run(runContext);
    }

    @Test
    @Disabled
    public void namedInsert() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());


        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 0; i < 5; i++) {
            FileSerde.write(output, ImmutableMap.builder()
                .put("tinyint", 125)
                .put("smallint", 32000)
                .put("integer", 2147483640)
                .put("bigint", 9007199254740991L)
                .put("decimal", new BigDecimal("1.55"))
                .put("float8", 1.12365125789541)
                .put("float", 1.2F)
                .put("char", "y")
                .put("varchar", "yoplait")
                .put("nchar", "tes")
                .put("nvarchar", "autret")
                .put("datetime", LocalDateTime.parse("1900-10-04T22:23:00.000"))
                .put("ansidate", LocalDate.parse("2006-05-16"))
                .put("ansidate2", LocalDate.parse("2006-05-16"))
                .put("timenotz", LocalTime.parse("04:05:30"))
                .put("timetz", LocalTime.parse("05:05:30"))
                .put("timelocaltz", LocalTime.parse("05:05:30"))
                .put("timestampnotz", LocalDateTime.parse("2006-05-16T00:00:00.000"))
                .put("timestamptz", ZonedDateTime.parse("2006-05-16T02:00:00.000+02:00[Europe/Paris]"))
                .put("timestamplocaltz", ZonedDateTime.parse("2006-05-16T02:00:00.000+02:00[Europe/Paris]"))
                .put("boolean", true).build()
            );

        }


        URI uri = storageInterface.put(URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        ArrayList<String> columns = new ArrayList<>();
        columns.add("tinyint");
        columns.add("datetime");
        columns.add("boolean");

        CopyIn task = CopyIn.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .from(uri.toString())
            .sql("insert into ingres.abdt_test values( ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ?)")
            .build();

        AbstractJdbcCopyIn.Output runOutput = task.run(runContext);
    }

    @Test
    @Disabled
    public void namedColumnsInsert() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());


        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 0; i < 5; i++) {
            FileSerde.write(output, ImmutableMap.builder()
                .put("tinyint", 125)
                .put("smallint", 32000)
                .put("integer", 2147483640)
                .put("bigint", 9007199254740991L)
                .put("decimal", new BigDecimal("1.55"))
                .put("float8", 1.12365125789541)
                .put("float", 1.2F)
                .put("char", "y")
                .put("varchar", "yoplait")
                .put("nchar", "tes")
                .put("nvarchar", "autret")
                .put("datetime", LocalDateTime.parse("1900-10-04T22:23:00.000"))
                .put("ansidate", LocalDate.parse("2006-05-16"))
                .put("ansidate2", LocalDate.parse("2006-05-16"))
                .put("timenotz", LocalTime.parse("04:05:30"))
                .put("timetz", LocalTime.parse("05:05:30"))
                .put("timelocaltz", LocalTime.parse("05:05:30"))
                .put("timestampnotz", LocalDateTime.parse("2006-05-16T00:00:00.000"))
                .put("timestamptz", ZonedDateTime.parse("2006-05-16T02:00:00.000+02:00[Europe/Paris]"))
                .put("timestamplocaltz", ZonedDateTime.parse("2006-05-16T02:00:00.000+02:00[Europe/Paris]"))
                .put("boolean", true).build()
            );

        }


        URI uri = storageInterface.put(URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        ArrayList<String> columns = new ArrayList<>();
        columns.add("tinyint");
        columns.add("datetime");
        columns.add("boolean");

        CopyIn task = CopyIn.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .from(uri.toString())
            .sql("insert into ingres.abdt_test(\"tinyint\",\"datetime\",\"boolean\") values( ? , ? , ? )")
            .columns(columns)
            .build();

        AbstractJdbcCopyIn.Output runOutput = task.run(runContext);
    }

    @Override
    protected String getUrl() {
        return this.url;
    }

    @Override
    protected String getUsername() {
        return this.user;
    }

    @Override
    protected String getPassword() {
        return this.password;
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/vectorwise.sql");
    }
}
