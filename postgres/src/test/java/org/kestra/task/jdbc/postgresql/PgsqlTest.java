package org.kestra.task.jdbc.postgresql;

import com.google.common.collect.ImmutableMap;
import io.micronaut.test.annotation.MicronautTest;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.Test;
import org.kestra.core.models.executions.Execution;
import org.kestra.core.models.flows.State;
import org.kestra.core.runners.RunContext;
import org.kestra.task.jdbc.AbstractJdbcQuery;
import org.kestra.task.jdbc.AbstractRdbmsTest;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.time.*;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * See :
 * - https://www.postgresql.org/docs/12/datatype.html
 */
@MicronautTest
public class PgsqlTest extends AbstractRdbmsTest {


    @Test
    void select() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        PostgresJdbcQuery task = PostgresJdbcQuery.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchOne(true)
            .timeZoneId("Europe/Paris")
            .sql("select concert_id, available, a, b, c, d, play_time, library_record, floatn_test, double_test, real_test, numeric_test, date_type, time_type, timez_type, timestamp_type, timestampz_type, interval_type, pay_by_quarter, schedule, json_type, blob_type from pgsql_types")
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get(0).get("concert_id"), is(1));
        // May be boolean
        assertThat(runOutput.getRow().get(0).get("available"), is(true));
        assertThat(runOutput.getRow().get(0).get("a"), is("four"));
        assertThat(runOutput.getRow().get(0).get("b"), is("This is a varchar"));
        assertThat(runOutput.getRow().get(0).get("c"), is("This is a text column data"));
        assertThat(runOutput.getRow().get(0).get("d"), nullValue());

        assertThat(runOutput.getRow().get(0).get("play_time"), is(32767));
        assertThat(runOutput.getRow().get(0).get("library_record"), is(9223372036854775807L));

        // See : https://github.com/pgjdbc/pgjdbc/issues/100 (money type in postgresql)
        // assertThat(runOutput.getRow().get(0).get("money_type"), is(999999.999));
        // Not equal to input value (Float and Double are for "Approximate Value"
        assertThat(runOutput.getRow().get(0).get("floatn_test"), not(9223372036854776000F));
        assertThat(runOutput.getRow().get(0).get("double_test"), is(9223372036854776000d));
        // Not equal to input value (Float and Double are for "Approximate Value"
        assertThat(runOutput.getRow().get(0).get("real_test"), not(9223372036854776000d));
        assertThat(runOutput.getRow().get(0).get("numeric_test"), is("2147483645.1234"));

        assertThat(runOutput.getRow().get(0).get("date_type"), is(LocalDate.parse("2030-12-25")));
        assertThat(runOutput.getRow().get(0).get("time_type"), is(LocalTime.parse("04:05:30")));
        assertThat(runOutput.getRow().get(0).get("timez_type"), is(LocalTime.parse("13:05:06")));
        assertThat(runOutput.getRow().get(0).get("timestamp_type"), is(LocalDateTime.parse("2004-10-19T10:23:54.999999")));
        assertThat(runOutput.getRow().get(0).get("timestampz_type"), is(ZonedDateTime.parse("2004-10-19T08:23:54.250+02:00[Europe/Paris]")));

        assertThat(runOutput.getRow().get(0).get("interval_type"), is("P10Y4M5DT0H0M10S"));
        assertThat(runOutput.getRow().get(0).get("pay_by_quarter"), is(new int[]{100, 200, 300}));
        assertThat(runOutput.getRow().get(0).get("schedule"), is(new String[][]{new String[]{"meeting", "lunch"}, new String[]{"training", "presentation"}}));

        assertThat(runOutput.getRow().get(0).get("json_type"), is("{\"color\":\"red\",\"value\":\"#f00\"}"));


        assertThat(runOutput.getRow().get(0).get("blob_type"), is(Hex.decodeHex("DEADBEEF".toCharArray())));
    }

    @Test
    void selectWithCompositeType() throws Exception {

        executeSqlScript("scripts/postgres.sql");
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        PostgresJdbcQuery task = PostgresJdbcQuery.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchOne(true)
            .sql("select item from pgsql_types") // PG SQL composite field are not supported
            .build();


        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            AbstractJdbcQuery.Output runOutput = task.run(runContext);
        });

    }

    public static final Map<String, String> INPUTS = ImmutableMap.of();

    @Test
    void updateFromFlow() throws Exception {
        Execution execution = runnerUtils.runOne(
            "org.kestra.jdbc.postgres",
            "update_postgres",
            null,
            (flow, exec) -> runnerUtils.typedInputs(flow, exec, INPUTS),
            Duration.ofMinutes(5)
        );

        assertThat(execution.getTaskRunList(), hasSize(2));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }

    @Override
    protected String getUrl() {
        return "jdbc:postgresql://127.0.0.1:56982/";
    }

    @Override
    protected String getUsername() {
        return "postgres";
    }

    @Override
    protected String getPassword() {
        return "pg_passwd";
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/postgres.sql");
    }
}
