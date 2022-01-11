package io.kestra.plugin.jdbc.redshift;

import com.google.common.collect.ImmutableMap;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.time.*;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


@MicronautTest
public class RedshiftTest extends AbstractRdbmsTest {
    @Value("${redshift.url}")
    protected String url;


    @Value("${redshift.user}")
    protected String user;

    @Value("${redshift.password}")
    protected String password;

    @SuppressWarnings("unchecked")
    @Test
    @Disabled("no server for unit test")
    void select() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchOne(true)
            .timeZoneId("Europe/Paris")
            .sql("select * from pgsql_types")
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get("available"), is(true));
        assertThat(runOutput.getRow().get("a"), is("four"));
        assertThat(runOutput.getRow().get("b"), is("This is a varchar"));
        assertThat(runOutput.getRow().get("c"), is("This is a text column data"));
        assertThat(runOutput.getRow().get("d"), nullValue());

        assertThat(runOutput.getRow().get("play_time"), is(32767));
        assertThat(runOutput.getRow().get("library_record"), is(9223372036854775807L));

        // Not equal to input value (Float and Double are for "Approximate Value"
        assertThat(runOutput.getRow().get("floatn_test"), not(9223372036854776000F));
        assertThat(runOutput.getRow().get("double_test"), is(9223372036854776000d));
        // Not equal to input value (Float and Double are for "Approximate Value"
        assertThat(runOutput.getRow().get("real_test"), not(9223372036854776000d));
        // redshift truncate .1234
        assertThat(runOutput.getRow().get("numeric_test"), is(new BigDecimal("2147483645")));

        assertThat(runOutput.getRow().get("date_type"), is(LocalDate.parse("2030-12-25")));
        assertThat(runOutput.getRow().get("time_type"), is(LocalTime.parse("04:05:30")));
        assertThat(runOutput.getRow().get("timez_type"), is(OffsetTime.parse("12:05:06+01:00")));
        assertThat(runOutput.getRow().get("timestamp_type"), is(LocalDateTime.parse("2004-10-19T10:23:54.999999")));
        assertThat(runOutput.getRow().get("timestampz_type"), is(ZonedDateTime.parse("2004-10-19T10:23:54+02:00[Europe/Paris]")));

        assertThat((List<Integer>) runOutput.getRow().get("pay_by_quarter"), containsInAnyOrder(100, 200, 300));
        assertThat((List<Map<String, String>>) runOutput.getRow().get("schedule"), containsInAnyOrder(Map.of("type", "meeting", "name", "lunch"), Map.of("type", "training", "name", "presentation")));
    }

    @Test
    @Disabled("no server for unit test")
    void update() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query taskUpdate = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchOne(true)
            .timeZoneId("Europe/Paris")
            .sql("update pgsql_types set d = 'D'")
            .build();

        taskUpdate.run(runContext);

        Query taskGet = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchOne(true)
            .timeZoneId("Europe/Paris")
            .sql("select d from pgsql_types")
            .build();

        AbstractJdbcQuery.Output runOutput = taskGet.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());
        assertThat(runOutput.getRow().get("d"), is("D"));
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
         executeSqlScript("scripts/redshift.sql");
    }
}
