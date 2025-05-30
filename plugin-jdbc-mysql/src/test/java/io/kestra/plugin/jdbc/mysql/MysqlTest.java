package io.kestra.plugin.jdbc.mysql;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.utils.IdUtils;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.time.*;
import java.util.Map;
import java.util.stream.Stream;

import static io.kestra.core.models.tasks.common.FetchType.FETCH_ONE;
import static io.kestra.core.models.tasks.common.FetchType.STORE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * See : https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-type-conversions.html
 */
@KestraTest
public class MysqlTest extends AbstractRdbmsTest {

    @Test
    void aliasQuery() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(Property.of(getUrl()))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .fetchType(Property.of(FETCH_ONE))
            .timeZoneId(Property.of("Europe/Paris"))
            .sql(Property.of("""
                 select concert_id as myConcertId,
                 a as char_column,
                 b as varchar_column,
                 c as text_column,
                 d as null_column,
                 date_type as date_column,
                 datetime_type as datetime_column,
                 timestamp_type as timestamp_column
                 from mysql_types
             """))
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get("myConcertId"), is("1"));

        assertThat(runOutput.getRow().get("char_column"), is("four"));
        assertThat(runOutput.getRow().get("varchar_column"), is("This is a varchar"));
        assertThat(runOutput.getRow().get("text_column"), is("This is a text column data"));
        assertThat(runOutput.getRow().get("null_column"), nullValue());

        assertThat(runOutput.getRow().get("date_column"), is(LocalDate.parse("2030-12-25")));
        assertThat(runOutput.getRow().get("datetime_column"), is(LocalDateTime.parse("2050-12-31T22:59:57.150150")));
        assertThat(runOutput.getRow().get("timestamp_column"), is(Instant.parse("2004-10-19T10:23:54.999999Z")));
    }

    @Test
    void select() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(Property.of(getUrl()))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .fetchType(Property.of(FETCH_ONE))
            .timeZoneId(Property.of("Europe/Paris"))
            .sql(Property.of("select * from mysql_types"))
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
        assertThat(runOutput.getRow().get("datetime_type"), is(LocalDateTime.parse("2050-12-31T22:59:57.150150")));
        assertThat(runOutput.getRow().get("time_type"), is(LocalTime.parse("04:05:30")));
        assertThat(runOutput.getRow().get("timestamp_type"), is(Instant.parse("2004-10-19T10:23:54.999999Z")));
        assertThat(runOutput.getRow().get("year_type"), is(LocalDate.parse("2025-01-01")));

        assertThat(runOutput.getRow().get("json_type"), is("{\"color\": \"red\", \"value\": \"#f00\"}"));
        assertThat(runOutput.getRow().get("blob_type"), is(Hex.decodeHex("DEADBEEF".toCharArray())));
    }

    @Test
    void selectQueryReturnNoValue_sizeShouldBeZero() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Query task = Query.builder()
            .url(Property.of(getUrl()))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .fetchType(Property.of(FETCH_ONE))
            .timeZoneId(Property.of("Europe/Paris"))
            .sql(Property.of("select * from mysql_types where concert_id='random'"))
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), nullValue());
        assertThat(runOutput.getSize(), equalTo(0L));
    }

    @ParameterizedTest
    @EnumSource(FetchType.class)
    void updateWithAllFetchType(FetchType fetchType) throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        final String randomString = IdUtils.create();

        Query taskUpdate = Query.builder()
            .url(Property.of(getUrl()))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .fetchType(Property.of(fetchType))
            .timeZoneId(Property.of("Europe/Paris"))
            .sql(Property.of(String.format("update mysql_types set c = '%s'", randomString)))
            .build();

        taskUpdate.run(runContext);

        Query taskGet = Query.builder()
            .url(Property.of(getUrl()))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .fetchType(Property.of(FETCH_ONE))
            .timeZoneId(Property.of("Europe/Paris"))
            .sql(Property.of("select c from mysql_types"))
            .build();

        AbstractJdbcQuery.Output runOutput = taskGet.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());
        assertThat(runOutput.getRow().get("c"), is(randomString));
    }

    @Test
    void selectWithParameters() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(Property.of(getUrl()))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .fetchType(Property.of(FETCH_ONE))
            .timeZoneId(Property.of("Europe/Paris"))
            .sql(Property.of("select * from mysql_types where concert_id=:concert_id"))
            .parameters(Property.of(Map.of("concert_id", "1")))
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get("concert_id"), is("1"));
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
            .sql(Property.of("select * from mysql_types where concert_id='random'"))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
    }

    public static Stream<Arguments> incorrectUrl() {
        return Stream.of(
            Arguments.of(new Property<>("")), // Empty URL
            Arguments.of(new Property<>("jdbc:postgresql://127.0.0.1:64790/kestra")) // Incorrect scheme
        );
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
