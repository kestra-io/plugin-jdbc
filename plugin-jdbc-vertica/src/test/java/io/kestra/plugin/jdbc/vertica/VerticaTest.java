package io.kestra.plugin.jdbc.vertica;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.Time;
import java.time.*;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * See : https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-type-conversions.html
 */
@KestraTest
public class VerticaTest extends AbstractRdbmsTest {
    @Test
    void select() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchOne(true)
            .timeZoneId("Europe/Paris")
            .sql("select * from vertica_types")
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());
        
        assertThat(runOutput.getRow().get("binary"), is(new byte[]{0b000101}));
        assertThat(runOutput.getRow().get("varbinary"), is(new byte[]{0b000101}));
        assertThat(runOutput.getRow().get("long_varbinary"), is(new byte[]{0b000101}));
        assertThat(runOutput.getRow().get("bytea"), is(new byte[]{0b000101}));
        assertThat(runOutput.getRow().get("raw"), is(new byte[]{0b000101}));
        assertThat(runOutput.getRow().get("boolean"), is(true));
        assertThat(runOutput.getRow().get("char"), is("four"));
        assertThat(runOutput.getRow().get("varchar"), is("four"));
        assertThat(runOutput.getRow().get("long_varchar"), is("four"));
        assertThat(runOutput.getRow().get("date"), is(LocalDate.parse("2030-12-25")));
        assertThat(runOutput.getRow().get("time"), is(Time.valueOf(LocalTime.parse("04:05:30"))));
        assertThat(runOutput.getRow().get("datetime"), is(LocalDateTime.parse("2004-10-19T10:23:54.999999")));
        assertThat(runOutput.getRow().get("smalldatetime"), is(LocalDateTime.parse("2004-10-19T10:23:54.999999")));
        assertThat(runOutput.getRow().get("time_with_timezone"), is(OffsetTime.parse("05:05:06+01:00")));
        assertThat(runOutput.getRow().get("timestamp"), is(LocalDateTime.parse("2004-10-19T10:23:54.999999")));
        assertThat(runOutput.getRow().get("timestamp_with_timezone"), is(ZonedDateTime.parse("1999-01-08T13:05:06+01:00[Europe/Paris]")));
        assertThat(runOutput.getRow().get("interval"), is(Duration.parse("PT719H59M59S")));
        assertThat(runOutput.getRow().get("interval_day_to_second"), is(Duration.parse("PT4H54.000775807S")));
        assertThat(runOutput.getRow().get("interval_year_to_month"), is(Period.parse("P1Y6M")));
        assertThat(runOutput.getRow().get("double_precision"), is(2147483645.1234));
        assertThat(runOutput.getRow().get("float"), is(2147483645.1234));
        assertThat(runOutput.getRow().get("float_n"), is(2147483645.1234));
        assertThat(runOutput.getRow().get("float8"), is(2147483645.1234));
        assertThat(runOutput.getRow().get("real"), is(2147483645.1234));
        assertThat(runOutput.getRow().get("integer"), is(123456789L));
        assertThat(runOutput.getRow().get("int"), is(123456789L));
        assertThat(runOutput.getRow().get("bigint"), is(123456789L));
        assertThat(runOutput.getRow().get("int8"), is(123456789L));
        assertThat(runOutput.getRow().get("smallint"), is(123L));
        assertThat(runOutput.getRow().get("tinyint"), is(1L));
        assertThat(runOutput.getRow().get("decimal"), is(new BigDecimal("2147483645.1234")));
        assertThat(runOutput.getRow().get("numeric"), is(new BigDecimal("2147483645.1234")));
        assertThat(runOutput.getRow().get("number"), is(new BigDecimal("2147483645.1234")));
        assertThat(runOutput.getRow().get("money"), is(new BigDecimal("2147483645.1234")));
        // @TODO: find a way to test, return byte[]
        // assertThat(runOutput.getRow().get("geometry"), is("POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))"));
        // assertThat(runOutput.getRow().get("geography"), is("POLYGON((1 2,3 4,2 3,1 2))"));
        assertThat(runOutput.getRow().get("uuid"), is("6bbf0744-74b4-46b9-bb05-53905d4538e7"));
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
            .sql("update vertica_types set varchar = 'D'")
            .build();

        taskUpdate.run(runContext);

        Query taskGet = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchOne(true)
            .timeZoneId("Europe/Paris")
            .sql("select varchar from vertica_types")
            .build();

        AbstractJdbcQuery.Output runOutput = taskGet.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());
        assertThat(runOutput.getRow().get("varchar"), is("D"));
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
        executeSqlScript("scripts/vertica.sql");
    }
}
