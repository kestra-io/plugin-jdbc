package io.kestra.plugin.jdbc.db2;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
public class Db2Test extends AbstractRdbmsTest {

    @Test
    void checkInitiation() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchOne(true)
            .sql("SELECT 1 AS result FROM SYSIBM.SYSDUMMY1")
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);

        assertThat(runOutput.getRow(), notNullValue());
        assertThat(runOutput.getSize(), equalTo(1L));
        assertThat(runOutput.getRow().get("RESULT"), equalTo(1));
    }

    @Test
    void select() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchOne(true)
            .timeZoneId("Europe/Paris")
            .sql("select * from db2_types")
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get("ID"), is(1));

        assertThat(runOutput.getRow().get("INTEGER_COL"), is(123));
        assertThat(runOutput.getRow().get("BIGINT_COL"), is(1234567890123456789L));
        assertThat(runOutput.getRow().get("DECIMAL_COL"), is(new BigDecimal("123.45")));
        assertThat(runOutput.getRow().get("REAL_COL"), is(123.45f));
        assertThat(runOutput.getRow().get("DOUBLE_COL"), is(123.45d));

        assertThat(runOutput.getRow().get("CHARACTER_COL"), is("c"));
        assertThat(runOutput.getRow().get("VARCHAR_COL"), is("var character"));
        assertThat(runOutput.getRow().get("GRAPHIC_COL"), is("g"));
        assertThat(runOutput.getRow().get("VARGRAPHIC_COL"), is("var graphic"));

        assertThat(runOutput.getRow().get("DATE_COL").toString(), is("2024-03-22"));
        assertThat(runOutput.getRow().get("DATE_COL"), is(LocalDate.of(2024, 3, 22)));

        assertThat(runOutput.getRow().get("TIME_COL").toString(), is("12:51:25"));
        assertThat(runOutput.getRow().get("TIME_COL"), is(LocalTime.parse("12:51:25")));

        assertThat(runOutput.getRow().get("TIMESTAMP_COL").toString(), is("2024-03-22T10:51:25Z"));
        assertThat(runOutput.getRow().get("TIMESTAMP_COL"), is(Instant.parse("2024-03-22T10:51:25.0Z")));

        assertThat(runOutput.getRow().get("BLOB_COL"), nullValue());
        assertThat(runOutput.getRow().get("CLOB_COL"), nullValue());
        assertThat(runOutput.getRow().get("XML_COL"), nullValue());
    }

    @Test
    void update() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query taskUpdate = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchOne(true)
            .timeZoneId("Europe/Paris")
            .sql("update db2_types set VARCHAR_col = 'VARCHAR_col'")
            .build();

        taskUpdate.run(runContext);

        Query taskGet = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchOne(true)
            .timeZoneId("Europe/Paris")
            .sql("select VARCHAR_col from db2_types")
            .build();

        AbstractJdbcQuery.Output runOutput = taskGet.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());
        assertThat(runOutput.getRow().get("VARCHAR_COL"), is("VARCHAR_col"));
    }

    @Test
    void updateBlob() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query taskUpdate = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchOne(true)
            .timeZoneId("Europe/Paris")
            .sql("update db2_types set BLOB_col = CAST('VARCHAR_col' AS BLOB)")
            .build();

        taskUpdate.run(runContext);

        Query taskGet = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchOne(true)
            .timeZoneId("Europe/Paris")
            .sql("select BLOB_col from db2_types")
            .build();

        AbstractJdbcQuery.Output runOutput = taskGet.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());
        assertThat(runOutput.getRow().get("BLOB_COL"), notNullValue());
    }

    @Override
    protected String getUrl() {
        return "jdbc:db2://localhost:50010/testdb";
    }

    @Override
    protected String getUsername() {
        return "db2inst1";
    }

    @Override
    protected String getPassword() {
        return "password";
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/db2.sql");
    }
}
