package io.kestra.plugin.jdbc.sqlite;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
public class SqliteTest extends AbstractRdbmsTest {
    @Test
    void select() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchOne(true)
            .timeZoneId("Europe/Paris")
            .sql("select * from lite_types")
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get("id"), is(1));

        // May be boolean
        assertThat(runOutput.getRow().get("boolean_column"), is(1));
        assertThat(runOutput.getRow().get("text_column"), is("Sample Text"));
        assertThat(runOutput.getRow().get("d"), nullValue());

        assertThat(runOutput.getRow().get("float_column"), is(3.14));
        assertThat(runOutput.getRow().get("double_column"), is(3.14159265359d));
        assertThat(runOutput.getRow().get("int_column"), is(42));

        assertThat(runOutput.getRow().get("date_column"), is(LocalDate.parse("2023-10-30")));
        assertThat(runOutput.getRow().get("datetime_column"), is(Instant.parse("2023-10-30T23:02:27.150Z")));
        assertThat(runOutput.getRow().get("time_column"), is(LocalTime.parse("14:30:00")));
        assertThat(runOutput.getRow().get("timestamp_column"), is(Instant.parse("2023-10-30T14:30:00.0Z")));
        assertThat(runOutput.getRow().get("year_column"), is(2023));

        assertThat(runOutput.getRow().get("json_column"), is("{\"key\": \"value\"}"));
        assertThat(runOutput.getRow().get("blob_column"), is(Hex.decodeHex("0102030405060708".toCharArray())));
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
            .sql("update lite_types set d = 'D'")
            .build();

        taskUpdate.run(runContext);

        Query taskGet = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchOne(true)
            .timeZoneId("Europe/Paris")
            .sql("select d from lite_types")
            .build();

        AbstractJdbcQuery.Output runOutput = taskGet.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());
        assertThat(runOutput.getRow().get("d"), is("D"));
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

        return DriverManager.getConnection(props.getProperty("jdbc.url"), props);
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/sqlite.sql");
    }
}
