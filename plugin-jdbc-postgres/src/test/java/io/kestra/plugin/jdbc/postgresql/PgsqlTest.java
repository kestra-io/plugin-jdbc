package io.kestra.plugin.jdbc.postgresql;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.FlowInputOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.tenant.TenantService;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.*;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static io.kestra.core.models.tasks.common.FetchType.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * See :
 * - https://www.postgresql.org/docs/12/datatype.html
 */
@KestraTest
public class PgsqlTest extends AbstractRdbmsTest {
    @Inject
    private FlowInputOutput flowIO;

    @Test
    void selectAndFetchOne() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(Property.of(TestUtils.url()))
            .username(Property.of(TestUtils.username()))
            .password(Property.of(TestUtils.password()))
            .ssl(Property.of(TestUtils.ssl()))
            .sslMode(Property.of(TestUtils.sslMode()))
            .sslRootCert(Property.of(TestUtils.ca()))
            .sslCert(Property.of(TestUtils.cert()))
            .sslKey(Property.of(TestUtils.keyNoPass()))
            .fetchType(Property.of(FETCH_ONE))
            .timeZoneId(Property.of("Europe/Paris"))
            .sql(Property.of("select concert_id, available, a, b, c, d, play_time, library_record, floatn_test, double_test, real_test, numeric_test, date_type, time_type, timez_type, timestamp_type, timestampz_type, interval_type, pay_by_quarter, schedule, json_type, jsonb_type, blob_type, tsvector_col, hstore_type from pgsql_types"))
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());
        checkRow(runOutput.getRow(), 1);
    }

    @Test
    void selectWithVoidObject() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(Property.of(TestUtils.url()))
            .username(Property.of(TestUtils.username()))
            .password(Property.of(TestUtils.password()))
            .ssl(Property.of(TestUtils.ssl()))
            .sslMode(Property.of(TestUtils.sslMode()))
            .sslRootCert(Property.of(TestUtils.ca()))
            .sslCert(Property.of(TestUtils.cert()))
            .sslKey(Property.of(TestUtils.keyNoPass()))
            .fetchType(Property.of(FETCH_ONE))
            .sql(Property.of("SELECT 'someString' as stringvalue, pg_sleep(0) as voidvalue"))
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());
        assertThat(runOutput.getRow().get("voidvalue"), nullValue());
        assertThat(runOutput.getRow().get("stringvalue"), is("someString"));
    }

    private void checkRow(Map<String, Object> row, int concertId) throws DecoderException {
        assertThat(row.get("concert_id"), is(concertId));
        // May be boolean
        assertThat(row.get("available"), is(true));
        assertThat(row.get("a"), is("four"));
        assertThat(row.get("b"), is("This is a varchar"));
        assertThat(row.get("c"), is("This is a text column data"));
        assertThat(row.get("d"), nullValue());

        assertThat(row.get("play_time"), is(32767));
        assertThat(row.get("library_record"), is(9223372036854775807L));

        // See : https://github.com/pgjdbc/pgjdbc/issues/100 (money type in postgresql)
        // assertThat(row.get("money_type"), is(999999.999));
        // Not equal to input value (Float and Double are for "Approximate Value"
        assertThat(row.get("floatn_test"), not(9223372036854776000F));
        assertThat(row.get("double_test"), is(9223372036854776000d));
        // Not equal to input value (Float and Double are for "Approximate Value"
        assertThat(row.get("real_test"), not(9223372036854776000d));
        assertThat(row.get("numeric_test"), is(new BigDecimal("2147483645.1234")));

        assertThat(row.get("date_type"), is(LocalDate.parse("2030-12-25")));
        assertThat(row.get("time_type"), is(LocalTime.parse("04:05:30")));
        assertThat(row.get("timez_type"), is(LocalTime.parse("13:05:06")));
        assertThat(row.get("timestamp_type"), is(LocalDateTime.parse("2004-10-19T10:23:54.999999")));
        assertThat(row.get("timestampz_type"), is(ZonedDateTime.parse("2004-10-19T08:23:54.250+02:00[Europe/Paris]")));

        assertThat(row.get("interval_type"), is("P10Y4M5DT0H0M10S"));
        assertThat(row.get("pay_by_quarter"), is(new int[]{100, 200, 300}));
        assertThat(row.get("schedule"), is(new String[][]{new String[]{"meeting", "lunch"}, new String[]{"training", "presentation"}}));

        assertThat(row.get("json_type"), is(Map.of("color", "red", "value", "#f00")));
        assertThat(row.get("jsonb_type"), is(Map.of("color", "blue", "value", "#0f0")));
        assertThat(row.get("blob_type"), is(Hex.decodeHex("DEADBEEF".toCharArray())));
        assertThat(row.get("tsvector_col"), is("'brown':4 'dice':2 'dog':9 'fox':5 'fuzzi':1 'jump':6 'lazi':8 'quick':3"));
        assertThat(row.get("hstore_type"), is(Map.of("foo", "bar", "hello", "world")));
    }

    @Test
    void selectAndFetch() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(Property.of(TestUtils.url()))
            .username(Property.of(TestUtils.username()))
            .password(Property.of(TestUtils.password()))
            .ssl(Property.of(TestUtils.ssl()))
            .sslMode(Property.of(TestUtils.sslMode()))
            .sslRootCert(Property.of(TestUtils.ca()))
            .sslCert(Property.of(TestUtils.cert()))
            .sslKey(Property.of(TestUtils.keyNoPass()))
            .fetchType(Property.of(FETCH))
            .timeZoneId(Property.of("Europe/Paris"))
            .sql(Property.of("select concert_id, available, a, b, c, d, play_time, library_record, floatn_test, double_test, real_test, numeric_test, date_type, time_type, timez_type, timestamp_type, timestampz_type, interval_type, pay_by_quarter, schedule, json_type, jsonb_type, blob_type, tsvector_col, hstore_type from pgsql_types"))
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRows(), notNullValue());
        assertThat(runOutput.getRows().size(), is(2));
        checkRow(runOutput.getRows().get(0), 1);
        checkRow(runOutput.getRows().get(1), 2);
    }

    @Test
    void selectAndFetchOne_EmptyResult() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Query task = Query.builder()
            .url(Property.of(TestUtils.url()))
            .username(Property.of(TestUtils.username()))
            .password(Property.of(TestUtils.password()))
            .ssl(Property.of(TestUtils.ssl()))
            .sslMode(Property.of(TestUtils.sslMode()))
            .sslRootCert(Property.of(TestUtils.ca()))
            .sslCert(Property.of(TestUtils.cert()))
            .sslKey(Property.of(TestUtils.keyNoPass()))
            .fetchType(Property.of(FETCH_ONE))
            .timeZoneId(Property.of("Europe/Paris"))
            .sql(Property.of("select concert_id from pgsql_types where b='random'"))
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), nullValue());
        assertThat(runOutput.getSize(), is(0L));
    }

    @Test
    void selectAndFetchToFile() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(Property.of(TestUtils.url()))
            .username(Property.of(TestUtils.username()))
            .password(Property.of(TestUtils.password()))
            .ssl(Property.of(TestUtils.ssl()))
            .sslMode(Property.of(TestUtils.sslMode()))
            .sslRootCert(Property.of(TestUtils.ca()))
            .sslCert(Property.of(TestUtils.cert()))
            .sslKey(Property.of(TestUtils.keyNoPass()))
            .fetchType(Property.of(STORE))
            .timeZoneId(Property.of("Europe/Paris"))
            .sql(Property.of("select concert_id, available, a, b, c, d, play_time, library_record, floatn_test, double_test, real_test, numeric_test, date_type, time_type, timez_type, timestamp_type, timestampz_type, interval_type, pay_by_quarter, schedule, json_type, jsonb_type, blob_type from pgsql_types"))
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getUri(), notNullValue());

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(this.storageInterface.get(null, null, runOutput.getUri())));
        int lines = 0;
        while (bufferedReader.readLine() != null) {
            lines++;
        }
        bufferedReader.close();
        assertThat(lines, is(2));
    }

    @Test
    void selectWithCompositeType() throws Exception {

        executeSqlScript("scripts/postgres.sql");
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(Property.of(TestUtils.url()))
            .username(Property.of(TestUtils.username()))
            .password(Property.of(TestUtils.password()))
            .ssl(Property.of(TestUtils.ssl()))
            .sslMode(Property.of(TestUtils.sslMode()))
            .sslRootCert(Property.of(TestUtils.ca()))
            .sslCert(Property.of(TestUtils.cert()))
            .sslKey(Property.of(TestUtils.keyNoPass()))
            .fetchType(Property.of(FETCH_ONE))
            .sql(Property.of("select item from pgsql_types")) // PG SQL composite field are not supported
            .build();


        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            task.run(runContext);
        });
    }

    public static final Map<String, Object> INPUTS = ImmutableMap.of(
        "sslRootCert", TestUtils.ca(),
        "sslCert", TestUtils.cert(),
        "sslKey", TestUtils.keyNoPass()
    );

    @Test
    void updateFromFlow() throws Exception {
        Execution execution = runnerUtils.runOne(
            TenantService.MAIN_TENANT,
            "io.kestra.jdbc.postgres",
            "update_postgres",
            null,
            (flow, exec) -> flowIO.readExecutionInputs(flow, exec, INPUTS),
            Duration.ofMinutes(5)
        );

        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
        assertThat(execution.getTaskRunList(), hasSize(2));
    }

    @ParameterizedTest
    @NullSource
    @MethodSource("incorrectUrl")
    void urlNotCorrectFormat_shouldThrowException(Property<String> url) {
        RunContext runContext = runContextFactory.of(Map.of());

        Query task = Query.builder()
            .url(url)
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .fetchType(Property.of(FETCH_ONE))
            .timeZoneId(Property.of("Europe/Paris"))
            .sql(Property.of("select item from pgsql_types;"))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
    }

    public static Stream<Arguments> incorrectUrl() {
        return Stream.of(
            Arguments.of(new Property<>("")), // Empty URL
            Arguments.of(new Property<>("jdbc:mysql://127.0.0.1:64790/kestra")) // Incorrect scheme
        );
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

        try {
            PostgresService.handleSsl(props, runContextFactory.of(), new PostgresConnection());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return DriverManager.getConnection(props.getProperty("jdbc.url"), props);
    }

    public static class PostgresConnection implements PostgresConnectionInterface {
        @Override
        public Property<String> getUrl() {
            return Property.of(TestUtils.url());
        }

        @Override
        public Property<String> getUsername() {
            return Property.of(TestUtils.username());
        }

        @Override
        public Property<String> getPassword() {
            return Property.of(TestUtils.password());
        }

        @Override
        public Property<Boolean> getSsl() {
            return Property.of(TestUtils.ssl());
        }

        @Override
        public Property<SslMode> getSslMode() {
            return Property.of(TestUtils.sslMode());
        }

        @Override
        public Property<String> getSslRootCert() {
            return Property.of(TestUtils.ca());
        }

        @Override
        public Property<String> getSslCert() {
            return Property.of(TestUtils.cert());
        }

        @Override
        public Property<String> getSslKey() {
            return Property.of(TestUtils.key());
        }

        @Override
        public Property<String> getSslKeyPassword() {
            return Property.of(TestUtils.keyPass());
        }

        @Override
        public void registerDriver() throws SQLException {

        }
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/postgres.sql");
    }
}