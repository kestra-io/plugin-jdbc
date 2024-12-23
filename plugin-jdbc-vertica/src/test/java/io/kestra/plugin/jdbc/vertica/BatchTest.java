package io.kestra.plugin.jdbc.vertica;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.Time;
import java.time.*;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
@Disabled("Issue with EOFException")
public class BatchTest extends AbstractRdbmsTest {

    @Test
    void insert() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 0; i < 5; i++) {
            FileSerde.write(output, List.of(
                new byte[]{0b000101},
                new byte[]{0b000101},
                new byte[]{0b000101},
                new byte[]{0b000101},
                new byte[]{0b000101},
                true,
                "four",
                "four",
                "four",
                LocalDate.parse("2030-12-25"),
                Time.valueOf(LocalTime.parse("04:05:30")),
                LocalDateTime.parse("2004-10-19T10:23:54.999999"),
                LocalDateTime.parse("2004-10-19T10:23:54.999999"),
                OffsetTime.parse("13:05:06+01:00"),
                LocalDateTime.parse("2004-10-19T10:23:54.999999"),
                ZonedDateTime.parse("1999-01-08T13:05:06+01:00[Europe/Paris]"),
                Duration.parse("PT719H59M59S"),
                Duration.parse("PT4H54.000775807S"),
                Period.parse("P1Y6M"),
                1.2F,
                2147483645.1234,
                2147483645.1234,
                2147483645.1234,
                2147483645.1234,
                123456789L,
                123456789L,
                123456789L,
                123456789L,
                123L,
                1L,
                new BigDecimal("2147483645.1234"),
                new BigDecimal("2147483645.1234"),
                new BigDecimal("2147483645.1234"),
                new BigDecimal("2147483645.1234")
            ));
        }

        URI uri = storageInterface.put(null, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Batch task = Batch.builder()
            .url(Property.of(getUrl()))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .from(Property.of(uri.toString()))
            .sql(Property.of("insert into vertica_types values( ? , ? , ? , ? , ? , ? , ? , ? , ?, ? , ? , ? , ? , ? , ? , ?, ? , ? , ? , ? , ?  , ? , ? , ? , ? , ?  , ? ,  ?  , ? , ? , ?, ? , ? , ? )"))
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

        URI uri = storageInterface.put(null, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

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

        for (int i = 0; i < 5; i++) {
            FileSerde.write(output, ImmutableMap.builder()
                .put("id", 125)
                .put("name", "kestra")
                .put("address", "here")
                .build()
            );
        }

        URI uri = storageInterface.put(null, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

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
            FileSerde.write(output, List.of(
                i,
                "It's-a me, Mario",
                "Here"
            ));
        }

        URI uri = storageInterface.put(null, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Batch task = Batch.builder()
            .url(Property.of(getUrl()))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .from(Property.of(uri.toString()))
            .table(Property.of("namedInsert"))
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

        URI uri = storageInterface.put(null, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

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
        return "jdbc:vertica://127.0.0.1:25433/docker";
    }

    @Override
    protected String getUsername() {
        return "dbadmin";
    }

    @Override
    protected String getPassword() {
        return "vertica_passwd";
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/vertica_insert.sql");
    }
}
