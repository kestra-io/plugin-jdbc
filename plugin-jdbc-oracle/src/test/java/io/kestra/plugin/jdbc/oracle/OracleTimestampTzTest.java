package io.kestra.plugin.jdbc.oracle;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

import static io.kestra.core.models.tasks.common.FetchType.FETCH;
import static io.kestra.core.models.tasks.common.FetchType.STORE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@Execution(ExecutionMode.SAME_THREAD)
public class OracleTimestampTzTest extends AbstractRdbmsTest {
    private static final List<ZonedDateTime> EXPECTED = List.of(
        ZonedDateTime.of(2024, 7, 15, 10, 30, 0, 123_456_000, ZoneId.of("America/New_York")),
        ZonedDateTime.of(2024, 1, 15, 8, 0, 0, 0, ZoneId.of("Europe/Berlin")),
        ZonedDateTime.of(2024, 3, 1, 12, 0, 0, 0, ZoneId.of("UTC")),
        ZonedDateTime.of(2024, 7, 15, 10, 30, 0, 123_456_000, ZoneOffset.ofHours(2))
    );

    private TimeZone previousDefault;

    @BeforeEach
    void saveDefaultTimeZone() {
        previousDefault = TimeZone.getDefault();
    }

    @AfterEach
    void restoreDefaultTimeZone() {
        TimeZone.setDefault(previousDefault);
    }

    @ParameterizedTest
    @ValueSource(strings = {"UTC", "Europe/Budapest"})
    void queryFetchKeepsRegion(String jvmZone) throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone(jvmZone));

        var output = query(FETCH);

        assertThat(zonedValues(output.getRows()), contains(EXPECTED.toArray()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"UTC", "Europe/Budapest"})
    void queryStoreKeepsRegion(String jvmZone) throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone(jvmZone));

        var output = query(STORE);

        assertThat(zonedValues(readIon(output.getUri())), contains(EXPECTED.toArray()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"UTC", "Europe/Budapest"})
    void batchPreservesInstantAndZone(String jvmZone) throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone(jvmZone));

        var stored = query(STORE);

        var batch = Batch.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .from(Property.ofValue(stored.getUri().toString()))
            .sql(Property.ofValue("insert into oracle_tstz_target values( ?, ? )"))
            .build();

        var output = batch.run(runContextFactory.of(ImmutableMap.of()));

        assertThat(output.getRowCount(), is(4L));
        assertThat(describe("oracle_tstz_target"), is(describe("oracle_tstz_source")));
        assertThat(describe("oracle_tstz_target"), contains(
            "1|2024-07-15 14:30:00.123456|AMERICA/NEW_YORK",
            "2|2024-01-15 07:00:00.000000|EUROPE/BERLIN",
            "3|2024-03-01 12:00:00.000000|UTC",
            "4|2024-07-15 08:30:00.123456|+02:00"
        ));
    }

    @Test
    void batchWritesOffsetDateTimeWithoutShift() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Budapest"));

        var output = new ByteArrayOutputStream();
        FileSerde.write(output, List.of(1, OffsetDateTime.parse("2024-07-15T10:30:00.123456-04:00")));
        var uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new ByteArrayInputStream(output.toByteArray()));

        var batch = Batch.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .from(Property.ofValue(uri.toString()))
            .sql(Property.ofValue("insert into oracle_tstz_target values( ?, ? )"))
            .build();

        batch.run(runContextFactory.of(ImmutableMap.of()));

        assertThat(describe("oracle_tstz_target"), contains("1|2024-07-15 14:30:00.123456|-04:00"));
    }

    @Test
    void batchFailsWithActionableMessageForUnknownRegion() throws Exception {
        var output = new ByteArrayOutputStream();
        FileSerde.write(output, List.of(1, ZonedDateTime.of(2024, 7, 15, 10, 30, 0, 0, ZoneId.of("Pacific/Kanton"))));
        var uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new ByteArrayInputStream(output.toByteArray()));

        var batch = Batch.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .from(Property.ofValue(uri.toString()))
            .sql(Property.ofValue("insert into oracle_tstz_target values( ?, ? )"))
            .build();

        var exception = assertThrows(Exception.class, () -> batch.run(runContextFactory.of(ImmutableMap.of())));

        assertThat(exception.getMessage(), containsString("Pacific/Kanton"));
        assertThat(exception.getMessage(), containsString("v$timezone_file"));
        assertThat(exception.getMessage(), containsString("DBMS_DST"));

        var oracleError = false;
        for (Throwable cause = exception.getCause(); cause != null; cause = cause.getCause()) {
            oracleError |= cause instanceof SQLException sql && sql.getErrorCode() == 1882;
        }
        assertThat(oracleError, is(true));
    }

    private AbstractJdbcQuery.Output query(FetchType fetchType) throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        var task = Query.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .fetchType(Property.ofValue(fetchType))
            .sql(Property.ofValue("select t_id, t_ts from oracle_tstz_source order by t_id"))
            .build();

        var output = task.run(runContext);
        assertThat(output, notNullValue());
        return output;
    }

    private List<Map<String, Object>> readIon(URI uri) throws Exception {
        var rows = new ArrayList<Map<String, Object>>();
        try (var in = storageInterface.get(TenantService.MAIN_TENANT, null, uri)) {
            FileSerde.read(in, row -> {
                @SuppressWarnings("unchecked")
                var map = (Map<String, Object>) row;
                rows.add(map);
            });
        }
        return rows;
    }

    private List<Object> zonedValues(List<Map<String, Object>> rows) {
        return rows.stream().map(row -> row.get("T_TS")).toList();
    }

    private List<String> describe(String table) throws SQLException {
        var lines = new ArrayList<String>();
        try (var connection = getConnection();
             var statement = connection.createStatement();
             var rs = statement.executeQuery(
                 "select t_id, to_char(sys_extract_utc(t_ts), 'YYYY-MM-DD HH24:MI:SS.FF6'), to_char(t_ts, 'TZR') from " + table + " order by t_id"
             )) {
            while (rs.next()) {
                lines.add(rs.getInt(1) + "|" + rs.getString(2) + "|" + rs.getString(3));
            }
        }
        return lines;
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
        try (var connection = getConnection()) {
            RunScript.execute(connection, new StringReader("DROP TABLE oracle_tstz_source;DROP TABLE oracle_tstz_target;"));
        } catch (Exception ignored) {
        }

        var url = Objects.requireNonNull(getClass().getClassLoader().getResource("scripts/oracle_timestamptz.sql"));
        try (var connection = getConnection(); var reader = new FileReader(new File(url.toURI()))) {
            RunScript.execute(connection, reader);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
