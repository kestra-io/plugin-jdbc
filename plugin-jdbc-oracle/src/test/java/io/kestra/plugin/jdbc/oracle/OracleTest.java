package io.kestra.plugin.jdbc.oracle;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import io.kestra.core.junit.annotations.KestraTest;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;

import java.io.FileNotFoundException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.stream.Stream;

import static io.kestra.core.models.tasks.common.FetchType.FETCH_ONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * See :
 * - https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT1843
 */
@KestraTest
public class OracleTest extends AbstractRdbmsTest {
    @Test
    void select() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(Property.of(getUrl()))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .fetchType(Property.of(FETCH_ONE))
            .timeZoneId(Property.of("Europe/Paris"))
            .sql(Property.of("select * from oracle_types"))
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get("T_NULL"), is(nullValue()));
        assertThat(runOutput.getRow().get("T_CHAR"), is("aa"));
        assertThat(runOutput.getRow().get("T_VARCHAR"), is("bb"));
        assertThat(runOutput.getRow().get("T_VARCHAR2"), is("cc"));
        assertThat(runOutput.getRow().get("T_NVARCHAR"), is("dd"));
        assertThat(runOutput.getRow().get("T_NVARCHAR2"), is("ee"));
        assertThat(runOutput.getRow().get("T_BLOB"), is("ff".getBytes(StandardCharsets.UTF_8)));
        assertThat(runOutput.getRow().get("T_CLOB"), is("gg"));
        assertThat(runOutput.getRow().get("T_NCLOB"), is("hh"));
        // assertThat(runOutput.getRow().get("T_BFILE"), is(Map.of("name", "STUFF", "bytes", "WD.pdf".getBytes(StandardCharsets.UTF_8))));
        assertThat(runOutput.getRow().get("T_NUMBER"), is(BigDecimal.valueOf(7456123.89)));
        assertThat(runOutput.getRow().get("T_NUMBER_1"), is(BigDecimal.valueOf(7456123.9)));
        assertThat(runOutput.getRow().get("T_NUMBER_2"), is(BigDecimal.valueOf(7456124)));
        assertThat(runOutput.getRow().get("T_NUMBER_3") , is(BigDecimal.valueOf(7456123.89)));
        assertThat(runOutput.getRow().get("T_NUMBER_4") ,is(BigDecimal.valueOf(7456123.9)));
        assertThat(runOutput.getRow().get("T_NUMBER_5"), is(BigDecimal.valueOf(7456100)));
        assertThat(runOutput.getRow().get("T_BINARY_FLOAT"), is(7456123.89F));
        assertThat(runOutput.getRow().get("T_BINARY_DOUBLE"), is(7456123.89D));
        assertThat(runOutput.getRow().get("T_DATE"), is(LocalDate.parse("1992-11-13")));
        assertThat(runOutput.getRow().get("T_TIMESTAMP"), is(LocalDateTime.parse("1998-01-23T06:00:00")));
        assertThat(runOutput.getRow().get("T_TIMESTAMP_TIME_ZONE"), is(ZonedDateTime.parse("1998-01-23T06:00:00-05:00")));
        assertThat(runOutput.getRow().get("T_TIMESTAMP_LOCAL"), is(LocalDateTime.parse("1998-01-23T12:00:00")));
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
            .parameters(Property.of(Map.of("char", "aa")))
            .sql(Property.of("select * from oracle_types where T_CHAR = :char"))
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get("T_CHAR"), is("aa"));
    }

    @Test
    void update() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query taskUpdate = Query.builder()
            .url(Property.of(getUrl()))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .fetchType(Property.of(FETCH_ONE))
            .timeZoneId(Property.of("Europe/Paris"))
            .sql(Property.of("UPDATE oracle_types SET T_VARCHAR = 'D' WHERE T_VARCHAR = 'bb'"))
            .build();

        taskUpdate.run(runContext);


        Query taskGet = Query.builder()
            .url(Property.of(getUrl()))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .fetchType(Property.of(FETCH_ONE))
            .timeZoneId(Property.of("Europe/Paris"))
            .sql(Property.of("select T_VARCHAR from oracle_types"))
            .build();

        AbstractJdbcQuery.Output runOutput = taskGet.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());
        assertThat(runOutput.getRow().get("T_VARCHAR"), is("D"));
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
            .sql(Property.of("select * from oracle_types;"))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
    }

    @Test
    void batchInsert_withZonedDateTime_shouldSucceed() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        try (var statement = getConnection().createStatement()) {
            statement.execute("CREATE TABLE ZONED_TEST (id NUMBER, ts TIMESTAMP, PRIMARY KEY(id))");
        }

        java.io.File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        ZonedDateTime zonedDateTime = ZonedDateTime.of(
            LocalDateTime.of(2025, 6, 28, 10, 0, 0),
            java.time.ZoneId.of("Europe/Paris")
        );

        java.util.Map<String, Object> data = new java.util.LinkedHashMap<>();
        data.put("id", 100);
        data.put("ts", zonedDateTime);

        try (var writer = new java.io.BufferedWriter(new java.io.FileWriter(tempFile))) {
            writer.write(io.kestra.core.serializers.JacksonMapper.ofIon().writeValueAsString(data));
        }

        Batch batchTask = Batch.builder()
            .url(Property.ofValue(this.getUrl()))
            .username(Property.ofValue(this.getUsername()))
            .password(Property.ofValue(this.getPassword()))
            .from(Property.ofValue(runContext.storage().putFile(tempFile).toString()))
            .sql(Property.ofValue("INSERT INTO ZONED_TEST (id, ts) VALUES (:id, :ts)"))
            .build();

        batchTask.run(runContext);

        Query queryTask = Query.builder()
            .url(Property.ofValue(this.getUrl()))
            .username(Property.ofValue(this.getUsername()))
            .password(Property.ofValue(this.getPassword()))
            .sql(Property.ofValue("SELECT ts FROM ZONED_TEST WHERE id = 100"))
            .fetchType(Property.ofValue(FETCH_ONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .build();

        AbstractJdbcQuery.Output queryOutput = queryTask.run(runContext);

        assertThat(queryOutput.getRow(), notNullValue());

        LocalDateTime insertedTimestamp = (LocalDateTime) queryOutput.getRow().get("TS");
        assertThat(insertedTimestamp, is(zonedDateTime.withZoneSameInstant(java.time.ZoneId.of("Europe/Paris")).toLocalDateTime()));
    }

    public static Stream<Arguments> incorrectUrl() {
        return Stream.of(
            Arguments.of(new Property<>("")), // Empty URL
            Arguments.of(new Property<>("jdbc:postgresql://127.0.0.1:64790/kestra")) // Incorrect scheme
        );
    }

    @Override
    protected String getUrl() {
        return "jdbc:oracle:thin:@localhost:49161:XE";
    }

    @Override
    protected String getUsername() {
        return "system";
    }

    @Override
    protected String getPassword() {
        return "oracle";
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        try {
            RunScript.execute(getConnection(), new StringReader("DROP TABLE oracle_types;"));
        } catch (Exception ignored) {

        }

        executeSqlScript("scripts/oracle.sql");
    }
}
