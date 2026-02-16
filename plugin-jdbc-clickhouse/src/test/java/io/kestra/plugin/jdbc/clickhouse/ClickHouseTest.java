package io.kestra.plugin.jdbc.clickhouse;

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
import java.net.URISyntaxException;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;

import java.io.*;
import java.math.BigDecimal;
import java.net.URI;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.kestra.core.models.tasks.common.FetchType.FETCH;
import static io.kestra.core.models.tasks.common.FetchType.FETCH_ONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class ClickHouseTest extends AbstractClickHouseTest {
    @SuppressWarnings("unchecked")
    @Test
    void select() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("select * from clickhouse_types"))
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get("Int8"), is(123));
        assertThat(runOutput.getRow().get("Float32"), is(2147483645.1234F));
        assertThat(runOutput.getRow().get("Float64"), is(2147483645.1234D));
        assertThat(runOutput.getRow().get("Decimal"), is(new BigDecimal("2147483645.1234")));
        assertThat(runOutput.getRow().get("String"), is("four"));
        assertThat(runOutput.getRow().get("FixedString"), is("four"));
        assertThat(runOutput.getRow().get("Uuid"), is("6bbf0744-74b4-46b9-bb05-53905d4538e7"));
        assertThat(runOutput.getRow().get("Date"), is(LocalDate.parse("2030-12-25")));
        assertThat(runOutput.getRow().get("DateTime"), is(ZonedDateTime.parse("2004-10-19T10:23:54+04:00[Europe/Moscow]")));
        assertThat(runOutput.getRow().get("DateTimeNoTZ"), is(LocalDateTime.parse("2004-10-19T10:23:54")));
        assertThat(runOutput.getRow().get("DateTime64"), is(ZonedDateTime.parse("2004-10-19T10:23:54.999+04:00[Europe/Moscow]")));
        assertThat(runOutput.getRow().get("Enum"), is("hello"));
        assertThat(runOutput.getRow().get("LowCardinality"), is("four"));
        assertThat(runOutput.getRow().get("Array"), is(new String[]{"a", "b"}));
        assertThat(runOutput.getRow().get("Nested.NestedId"), is(new Byte[] {123}));
        assertThat(runOutput.getRow().get("Nested.NestedString"), is(new String[]{"four"}));
        assertThat((List<Object>) runOutput.getRow().get("Tuple"), containsInAnyOrder("a", Byte.valueOf("1")));
        assertThat(runOutput.getRow().get("Nullable"), is(nullValue()));
        assertThat(runOutput.getRow().get("Ipv4"), is("116.253.40.133"));
        assertThat(runOutput.getRow().get("Ipv6"), is("2a02:aa08:e000:3100:0:0:0:2"));
        assertThat(runOutput.getRow().get("UInt16"), is(65534));
    }

    @Test
    void selectWithParameters() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .parameters(Property.ofValue(Map.of("num", 123)))
            .sql(Property.ofValue("select * from clickhouse_types where Int8 = :num"))
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get("Int8"), is(123));
    }

    @Test
    void update() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query taskUpdate = Query.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .fetchType(Property.ofValue(FetchType.NONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("ALTER TABLE clickhouse_types UPDATE String = 'D' WHERE String = 'four'"))
            .build();

        taskUpdate.run(runContext);

        // clickhouse need some to refresh
        Thread.sleep(500);

        Query taskGet = Query.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("select String from clickhouse_types"))
            .build();

        AbstractJdbcQuery.Output runOutput = taskGet.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());
        assertThat(runOutput.getRow().get("String"), is("D"));
    }

    @Test
    void updateBatch() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 0; i < 1000; i++) {
            FileSerde.write(output, ImmutableMap.builder()
                .put("String", "kestra")
                .build()
            );
        }

        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        BulkInsert taskUpdate = BulkInsert.builder()
            .from(Property.ofValue(uri.toString()))
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            // If you want to specify SETTINGS for INSERT query then you have to do it before the FORMAT clause since everything after FORMAT format_name is treated as data
            // See: https://clickhouse.com/docs/sql-reference/statements/insert-into
            .sql(Property.ofValue("INSERT INTO clickhouse_types (String) SETTINGS async_insert=1, wait_for_async_insert=1 VALUES(?)"))
            .build();

        taskUpdate.run(runContext);

        // clickhouse need some to refresh
        Thread.sleep(500);

        Query taskGet = Query.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .fetchType(Property.ofValue(FETCH))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("select String from clickhouse_types"))
            .build();

        AbstractJdbcQuery.Output runOutput = taskGet.run(runContext);
        assertThat(runOutput.getRows(), notNullValue());
        assertThat(runOutput.getSize(), is(1001L));
        assertThat(runOutput.getRows().stream().anyMatch(map -> map.get("String").equals("kestra")), is(true));
    }

    @Test
    void updateNullableInt8() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query taskUpdate = Query.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .fetchType(Property.ofValue(FetchType.NONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("ALTER TABLE clickhouse_types UPDATE Nullable = 123 WHERE Nullable IS NULL"))
            .build();

        taskUpdate.run(runContext);

        // clickhouse need some to refresh
        Thread.sleep(500);

        Query taskGet = Query.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("select Nullable from clickhouse_types"))
            .build();

        AbstractJdbcQuery.Output runOutput = taskGet.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());
        assertThat(runOutput.getRow().get("Nullable"), is(123));
    }

    @Test
    public void noSqlForInsert() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 1; i < 6; i++) {
            FileSerde.write(output, Arrays.asList(
                i,
                2147483645.1234,
                2147483645.1234,
                BigDecimal.valueOf(2147483645.1234),
                "four"
            ));
        }

        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        BulkInsert task = BulkInsert.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .from(Property.ofValue(uri.toString()))
            .table(Property.ofValue("clickhouse_ins"))
            .build();

        AbstractJdbcBatch.Output runOutput = task.run(runContext);

        assertThat(runOutput.getRowCount(), is(5L));
    }

    @Test
    public void noSqlWithNamedColumnsForInsert() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 1; i < 6; i++) {
            FileSerde.write(output, List.of(
                "Mario"
            ));
        }

        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        BulkInsert task = BulkInsert.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .from(Property.ofValue(uri.toString()))
            .table(Property.ofValue("clickhouse_types"))
            .columns(Property.ofValue(List.of("String")))
            .build();

        AbstractJdbcBatch.Output runOutput = task.run(runContext);

        assertThat(runOutput.getRowCount(), is(5L));
    }

    @ParameterizedTest
    @NullSource
    @MethodSource("incorrectUrl")
    void urlNotCorrectFormat_shouldThrowException(Property<String> url) {
        RunContext runContext = runContextFactory.of(Map.of());

        Query task = Query.builder()
            .url(url)
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .fetchType(Property.ofValue(FETCH_ONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("select String from clickhouse_types"))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
    }

    public static Stream<Arguments> incorrectUrl() {
        return Stream.of(
            Arguments.of(new Property<>("")), //Empty URL
            Arguments.of(new Property<>("jdbc:postgresql://127.0.0.1:64790/kestra")) // Incorrect scheme
        );
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/clickhouse.sql");
    }
}
