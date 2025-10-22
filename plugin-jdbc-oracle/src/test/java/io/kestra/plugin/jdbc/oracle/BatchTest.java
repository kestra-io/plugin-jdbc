package io.kestra.plugin.jdbc.oracle;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import io.kestra.core.junit.annotations.KestraTest;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
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

        for (int i = 0; i < 5; i++) {
            FileSerde.write(output, List.of(
                "aa",
                "",
                "bb",
                "cc",
                "dd",
                "ee",
                "ff".getBytes(StandardCharsets.UTF_8),
                "gg",
                "hh",
                BigDecimal.valueOf(7456123.89),
                BigDecimal.valueOf(7456123.9),
                BigDecimal.valueOf(7456124),
                BigDecimal.valueOf(7456123.89),
                BigDecimal.valueOf(7456123.9),
                BigDecimal.valueOf(7456100),
                7456123.89F,
                7456123.89D,
                LocalDate.parse("1992-11-13"),
                LocalDateTime.parse("1998-01-23T06:00:00"),
                ZonedDateTime.parse("1998-01-23T06:00:00-05:00"),
                LocalDateTime.parse("1998-01-23T12:00:00")
            ));
        }

        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Batch task = Batch.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .from(Property.ofValue(uri.toString()))
            .sql(Property.ofValue("insert into oracle_types values( ? , ? , ?, ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? )"))
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
                .put("t_id",i)
                .put("t_name","Kestra")
                .put("t_address","here").build()
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

        for (int i = 0; i < 5; i++) {
            FileSerde.write(output, ImmutableMap.builder()
                .put("t_id",i)
                .put("t_name","Kestra")
                .build()
            );
        }

        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        ArrayList<String> columns = new ArrayList<>();
        columns.add("t_id");
        columns.add("t_name");

        Batch task = Batch.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .from(Property.ofValue(uri.toString()))
            .sql(Property.ofValue("insert into namedInsert (t_id,t_name) values( ? , ? )"))
            .columns(Property.ofValue(columns))
            .build();

        AbstractJdbcBatch.Output runOutput = task.run(runContext);

        assertThat(runOutput.getRowCount(), is(5L));
    }

    @Test
    public void noSqlForInsert() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 0; i < 5; i++) {
            FileSerde.write(output, List.of(
                "aa",
                "",
                "bb",
                "cc",
                "dd",
                "ee",
                "ff".getBytes(StandardCharsets.UTF_8),
                "gg",
                "hh",
                BigDecimal.valueOf(7456123.89),
                BigDecimal.valueOf(7456123.9),
                BigDecimal.valueOf(7456124),
                BigDecimal.valueOf(7456123.89),
                BigDecimal.valueOf(7456123.9),
                BigDecimal.valueOf(7456100),
                7456123.89F,
                7456123.89D,
                LocalDate.parse("1992-11-13"),
                LocalDateTime.parse("1998-01-23T06:00:00"),
                ZonedDateTime.parse("1998-01-23T06:00:00-05:00"),
                LocalDateTime.parse("1998-01-23T12:00:00")
            ));
        }

        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Batch task = Batch.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .from(Property.ofValue(uri.toString()))
            .table(Property.ofValue("ORACLE_TYPES"))
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
	            "It's-a me, Mario"
            ));
        }

        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Batch task = Batch.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .from(Property.ofValue(uri.toString()))
            .table(Property.ofValue("namedInsert"))
            .columns(Property.ofValue(List.of("t_name")))
            .build();

        AbstractJdbcBatch.Output runOutput = task.run(runContext);

        assertThat(runOutput.getRowCount(), is(5L));
    }

    @Test
    public void shouldRespectColumnOrder() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        // Create .ion file with columns intentionally reversed in the data
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        OutputStream output = new FileOutputStream(tempFile);
        FileSerde.write(output, ImmutableMap.builder()
            .put("t_name", "Kestra")
            .put("t_id", 123)
            .build()
        );

        URI uri = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new FileInputStream(tempFile)
        );

        // Explicit column order: t_id first
        Batch task = Batch.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .from(Property.ofValue(uri.toString()))
            .sql(Property.ofValue("insert into namedInsert (t_id, t_name) values(?, ?)"))
            .columns(Property.ofValue(List.of("t_id", "t_name")))
            .build();

        task.run(runContext);

        var conn = getConnection();
        var stmt = conn.prepareStatement("SELECT t_id, t_name FROM namedInsert WHERE t_id = 123");
        var rs = stmt.executeQuery();

        assertThat("Should have at least one row", rs.next(), is(true));
        assertThat("Correct value for t_id", rs.getString("t_id"), is("123"));
        assertThat("Correct value for t_name", rs.getString("t_name"), is("Kestra"));

        rs.close();
        stmt.close();
        conn.close();
    }

    @Test
    public void shouldInsertNullWhenColumnMissing() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        // Create a temporary ION file with missing column ("t_address" omitted)
        File tempFile = File.createTempFile("missing_column_", ".trs");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            for (int i = 0; i < 3; i++) {
                FileSerde.write(output, ImmutableMap.of(
                    "t_id", i,
                    "t_name", "Kestra_" + i
                    // "t_address" is intentionally missing
                ));
            }
        }

        URI uri = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new FileInputStream(tempFile)
        );

        // Task: insert into table expecting (t_id, t_name, t_address)
        Batch task = Batch.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .from(Property.ofValue(uri.toString()))
            .sql(Property.ofValue("INSERT INTO namedInsert (t_id, t_name, t_address) VALUES (?, ?, ?)"))
            .columns(Property.ofValue(List.of("t_id", "t_name", "t_address")))
            .build();

        boolean succeededWithoutOra17041 = false;

        try {
            AbstractJdbcBatch.Output runOutput = task.run(runContext);
            succeededWithoutOra17041 = true;
            assertThat(runOutput.getRowCount(), is(3L));
        } catch (Exception e) {
            System.out.println("Caught exception: " + e.getMessage());
            succeededWithoutOra17041 = !e.getMessage().contains("ORA-17041");
        }

        assertThat("Should not trigger ORA-17041 when missing column (NULL should be inserted instead)",
            succeededWithoutOra17041, is(true));
    }

    @Override
    protected String getUrl() {
        return "jdbc:oracle:thin:@localhost:49161:XE";
    }

    @Override
    protected String getUsername() {
        return "system";
    }

    @Override
    protected String getPassword() {
        return "oracle";
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        try {
            RunScript.execute(getConnection(), new StringReader("DROP TABLE oracle_types;DROP TABLE namedInsert;"));
        } catch (Exception ignored) {
        }
        executeSqlScript("scripts/oracle_insert.sql");
    }
}
