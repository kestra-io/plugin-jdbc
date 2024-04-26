package io.kestra.plugin.jdbc.mysql;

import com.google.common.collect.ImmutableMap;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * See : https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-type-conversions.html
 */
@MicronautTest
public class MysqlTest extends AbstractRdbmsTest {

    @Test
    void aliasQuery() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchOne(true)
            .timeZoneId("Europe/Paris")
            .sql("select concert_id as myConcertId, a as char_column, b as varchar_column, c as text_column, d as null_column from mysql_types")
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get("myConcertId"), is("1"));

        assertThat(runOutput.getRow().get("char_column"), is("four"));
        assertThat(runOutput.getRow().get("varchar_column"), is("This is a varchar"));
        assertThat(runOutput.getRow().get("text_column"), is("This is a text column data"));
        assertThat(runOutput.getRow().get("null_column"), nullValue());
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
            .sql("select * from mysql_types")
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get("concert_id"), is("1"));

        // May be boolean
        assertThat(runOutput.getRow().get("available"), is(1));
        assertThat(runOutput.getRow().get("a"), is("four"));
        assertThat(runOutput.getRow().get("b"), is("This is a varchar"));
        assertThat(runOutput.getRow().get("c"), is("This is a text column data"));
        assertThat(runOutput.getRow().get("d"), nullValue());

        assertThat(runOutput.getRow().get("play_time"), is(-9223372036854775808L));
        assertThat(runOutput.getRow().get("library_record"), is(1844674407370955161L));
        assertThat(runOutput.getRow().get("bitn_test"), is(new byte[]{0b000101}));

        // Not equal to input value (Float and Double are for "Approximate Value"
        assertThat(runOutput.getRow().get("floatn_test"), is(9223372036854776000F));
        assertThat(runOutput.getRow().get("double_test"), is(9223372036854776000d));
        assertThat(runOutput.getRow().get("doublen_test"), is(2147483645.1234d));
        assertThat(runOutput.getRow().get("numeric_test"), is(new BigDecimal("5.36")));
        assertThat(runOutput.getRow().get("salary_decimal"), is(new BigDecimal("999.99")));

        assertThat(runOutput.getRow().get("date_type"), is(LocalDate.parse("2030-12-25")));
        assertThat(runOutput.getRow().get("datetime_type"), is(Instant.parse("2050-12-31T22:59:57.150150Z")));
        assertThat(runOutput.getRow().get("time_type"), is(LocalTime.parse("04:05:30")));
        assertThat(runOutput.getRow().get("timestamp_type"), is(Instant.parse("2004-10-19T10:23:54.999999Z")));
        assertThat(runOutput.getRow().get("year_type"), is(LocalDate.parse("2025-01-01")));

        assertThat(runOutput.getRow().get("json_type"), is("{\"color\": \"red\", \"value\": \"#f00\"}"));
        assertThat(runOutput.getRow().get("blob_type"), is(Hex.decodeHex("DEADBEEF".toCharArray())));
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
            .sql("update mysql_types set d = 'D'")
            .build();

        taskUpdate.run(runContext);

        Query taskGet = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchOne(true)
            .timeZoneId("Europe/Paris")
            .sql("select d from mysql_types")
            .build();

        AbstractJdbcQuery.Output runOutput = taskGet.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());
        assertThat(runOutput.getRow().get("d"), is("D"));
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
        executeSqlScript("scripts/mysql.sql");
    }
}
