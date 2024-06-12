package io.kestra.plugin.jdbc.snowflake;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@Disabled("no server for unit test")
public class SnowflakeTest extends AbstractRdbmsTest {
    @Value("${snowflake.host}")
    protected String host;

    @Value("${snowflake.username}")
    protected String username;

    @Value("${snowflake.password}")
    protected String password;

    @Test
    void select() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .warehouse("COMPUTE_WH")
            .database("UNITTEST")
            .schema("public")
            .fetchOne(true)
            .timeZoneId("Europe/Paris")
            .sql("select * from snowflake_types")
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        ArrayList<Object> arrays = new ArrayList<>();
        arrays.add(12);
        arrays.add("twelve");
        arrays.add(null);

        assertThat(runOutput.getRow().get("NU"), is(nullValue()));
        assertThat(runOutput.getRow().get("N"), is(2147483645L));
        assertThat(runOutput.getRow().get("NUM10"), is(BigDecimal.valueOf(123456.1)));
        assertThat(runOutput.getRow().get("DEC"), is(BigDecimal.valueOf(2147483645.12)));
        assertThat(runOutput.getRow().get("NUMERIC"), is(BigDecimal.valueOf(2147483645.123)));
        assertThat(runOutput.getRow().get("INT"), is(Long.parseLong("2147483645")));
        assertThat(runOutput.getRow().get("INTEGER"), is(2147483645L));
        assertThat(runOutput.getRow().get("F"), is(2147483645.1234d));
        assertThat(runOutput.getRow().get("DP"), is(2147483645.1234d));
        assertThat(runOutput.getRow().get("R"), is(2147483645.1234d));
        assertThat(runOutput.getRow().get("V50"), is("aa"));
        assertThat(runOutput.getRow().get("C"), is("b"));
        assertThat(runOutput.getRow().get("C10"), is("cc"));
        assertThat(runOutput.getRow().get("S"), is("dd"));
        assertThat(runOutput.getRow().get("S20"), is("ee"));
        assertThat(runOutput.getRow().get("T"), is("ff"));
        assertThat(runOutput.getRow().get("T30"), is("gg"));
        assertThat(runOutput.getRow().get("B"), is(true));
        assertThat(runOutput.getRow().get("D"), is(LocalDate.parse("2011-10-29")));
        assertThat(runOutput.getRow().get("TI"), is(LocalTime.parse("11:03:56")));
        assertThat(runOutput.getRow().get("TS"), is(ZonedDateTime.parse("2009-09-15T10:59:43+02:00[Europe/Paris]")));
        assertThat(runOutput.getRow().get("TS_TZ"), is(Instant.parse("2017-01-01T20:00:00Z")));
        assertThat(runOutput.getRow().get("TS_NTZ"), is(ZonedDateTime.parse("2014-01-02T16:00:00+01:00[Europe/Paris]")));
        assertThat(runOutput.getRow().get("TS_LTZ"), is(Instant.parse("2014-01-02T08:00:00Z")));
        assertThat(runOutput.getRow().get("V"), is(Map.of("key3", "value3", "key4", "value4")));
        assertThat(runOutput.getRow().get("O"), is(Map.of("age", 42, "name", "Jones")));
        assertThat(runOutput.getRow().get("A"), is(arrays));
        assertThat(runOutput.getRow().get("G"), is(Map.of("coordinates", List.of(-122.35, 37.55), "type", "Point")));
    }

    @Override
    protected String getUrl() {
        return "jdbc:snowflake://" + this.host + "/?loginTimeout=3";
    }

    @Override
    protected String getUsername() {
        return this.username;
    }

    @Override
    protected String getPassword() {
        return this.password;
    }

    @Override
    protected Connection getConnection() throws SQLException {
        Properties properties = new Properties();
        properties.put("user", getUsername());
        properties.put("password", getPassword());
        properties.put("warehouse", "COMPUTE_WH");
        properties.put("db", "UNITTEST");
        properties.put("schema", "public");

        return DriverManager.getConnection(getUrl(), properties);
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/snowflake.sql");
    }
}
