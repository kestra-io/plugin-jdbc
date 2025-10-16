package io.kestra.plugin.jdbc.duckdb;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import jakarta.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static io.kestra.core.models.tasks.common.FetchType.*;
import static io.kestra.plugin.jdbc.duckdb.DuckDbTestUtils.getCsvSourceUri;
import static io.kestra.plugin.jdbc.duckdb.DuckDbTestUtils.getDatabaseFileSourceUri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * See :
 * - https://duckdb.org/docs/sql/data_types/overview
 */
@KestraTest
class DuckDbTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    private static final String MOTHER_DUCK_TOKEN = "";

    @Test
    void selectFromExistingFileInUrl() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        URL resource = DuckDbTest.class.getClassLoader().getResource("db/duck.db");

        Query task = Query.builder()
            .fetchType(Property.ofValue(FETCH_ONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .url(Property.ofValue("jdbc:duckdb:" + Objects.requireNonNull(resource).getPath()))
            .sql(Property.ofValue("SELECT * FROM duck_types"))
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get("t_null"), is(nullValue()));
        assertThat(runOutput.getRow().get("t_bigint"), is(9223372036854775807L));
        assertThat(runOutput.getRow().get("t_boolean"), is(true));
        assertThat(runOutput.getRow().get("t_date"), is(LocalDate.parse("1992-09-20")));
        assertThat(runOutput.getRow().get("t_double"), is(12345.12345D));
        assertThat(runOutput.getRow().get("t_decimal"), is(BigDecimal.valueOf(12345123L, 3)));
        assertThat(runOutput.getRow().get("t_hugeint"), is("9223372036854775807"));
        assertThat(runOutput.getRow().get("t_integer"), is(2147483647));
        assertThat(runOutput.getRow().get("t_interval"), is("28 days"));
        assertThat(runOutput.getRow().get("t_real"), is(123.456F));
        assertThat(runOutput.getRow().get("t_smallint"), is((short) 127));
        assertThat(runOutput.getRow().get("t_timestamp"), is(ZonedDateTime.parse("2020-06-10T15:55:23.383345+02:00[Europe/Paris]")));
        assertThat(runOutput.getRow().get("t_timestamptz"), is(OffsetDateTime.parse("2001-08-22T10:04:05.321Z")));
        assertThat(runOutput.getRow().get("t_tinyint"), is(127));
        assertThat(runOutput.getRow().get("t_ubigint"), is("9223372036854775807"));
        assertThat(runOutput.getRow().get("t_uinteger"), is(2147483647L));
        assertThat(runOutput.getRow().get("t_usmallint"), is(32767));
        assertThat(runOutput.getRow().get("t_utinyint"), is((short) 127));
        assertThat(runOutput.getRow().get("t_varchar"), is("test"));
        assertThat(runOutput.getRow().get("t_enum"), is("happy"));
    }

    @Test
    void selectFromExistingFileInUrlWithParameters() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        URL resource = DuckDbTest.class.getClassLoader().getResource("db/duck.db");

        Query task = Query.builder()
            .fetchType(Property.ofValue(FETCH_ONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .url(Property.ofValue("jdbc:duckdb:" + Objects.requireNonNull(resource).getPath()))
            .parameters(Property.ofValue(Map.of("num", 2147483647)))
            .sql(Property.ofValue("SELECT * FROM duck_types WHERE t_integer = :num"))
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get("t_integer"), is(2147483647));
    }


    @Test
    void selectFromExistingFileInParameter() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        URL resource = DuckDbTest.class.getClassLoader().getResource("db/duck.db");

        Query task = Query.builder()
            .fetchType(Property.ofValue(FETCH_ONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .url(Property.ofValue("jdbc:duckdb:"))
            .databaseFile(Path.of(Objects.requireNonNull(resource).toURI()))
            .sql(Property.ofValue("SELECT * FROM duck_types"))
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get("t_null"), is(nullValue()));
        assertThat(runOutput.getRow().get("t_bigint"), is(9223372036854775807L));
        assertThat(runOutput.getRow().get("t_boolean"), is(true));
        assertThat(runOutput.getRow().get("t_date"), is(LocalDate.parse("1992-09-20")));
        assertThat(runOutput.getRow().get("t_double"), is(12345.12345D));
        assertThat(runOutput.getRow().get("t_decimal"), is(BigDecimal.valueOf(12345123L, 3)));
        assertThat(runOutput.getRow().get("t_hugeint"), is("9223372036854775807"));
        assertThat(runOutput.getRow().get("t_integer"), is(2147483647));
        assertThat(runOutput.getRow().get("t_interval"), is("28 days"));
        assertThat(runOutput.getRow().get("t_real"), is(123.456F));
        assertThat(runOutput.getRow().get("t_smallint"), is((short) 127));
        assertThat(runOutput.getRow().get("t_timestamp"), is(ZonedDateTime.parse("2020-06-10T15:55:23.383345+02:00[Europe/Paris]")));
        assertThat(runOutput.getRow().get("t_timestamptz"), is(OffsetDateTime.parse("2001-08-22T10:04:05.321Z")));
        assertThat(runOutput.getRow().get("t_tinyint"), is(127));
        assertThat(runOutput.getRow().get("t_ubigint"), is("9223372036854775807"));
        assertThat(runOutput.getRow().get("t_uinteger"), is(2147483647L));
        assertThat(runOutput.getRow().get("t_usmallint"), is(32767));
        assertThat(runOutput.getRow().get("t_utinyint"), is((short) 127));
        assertThat(runOutput.getRow().get("t_varchar"), is("test"));
        assertThat(runOutput.getRow().get("t_enum"), is("happy"));
    }

    static Stream<String> nullOrFilledDuckDbUrl() {
        return Stream.of(
            null,
            "jdbc:duckdb:/tmp/" + IdUtils.create() + ".db"
        );
    }

    @Disabled("Requires a Mother Duck token")
    @Test
    void selectWithMotherDuckUrl() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .fetchType(Property.ofValue(FETCH))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .url(Property.ofValue("jdbc:duckdb:md:my_db?motherduck_token=" + MOTHER_DUCK_TOKEN))
            .sql(Property.ofValue("""
                SELECT created_date, agency, complaint_type, landmark, resolution_description
                FROM sample_data.nyc.service_requests
                WHERE created_date >= '2021-01-01' AND created_date <= '2021-01-31' LIMIT 100;
                """))
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRows(), notNullValue());
        assertThat(runOutput.getRows().getFirst().size(), is(5));
    }

    @Disabled("Requires a Mother Duck token")
    @Test
    void selectWithMotherDuckUrlAndParameters() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .fetchType(Property.ofValue(FETCH))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .url(Property.ofValue("jdbc:duckdb:md:my_db?motherduck_token=" + MOTHER_DUCK_TOKEN))
            .parameters(Property.ofValue(Map.of(
                "type", "Street Condition"
            )))
            .sql(Property.ofValue("""
                SELECT created_date, agency, complaint_type, landmark, resolution_description
                FROM sample_data.nyc.service_requests
                WHERE complaint_type = :type LIMIT 100;
                """))
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRows(), notNullValue());
        assertThat(runOutput.getRows().size(), is(100));
        runOutput.getRows().forEach(row -> assertThat(row.get("complaint_type"), is("Street Condition")));
    }

    @Test
    void queryWithExistingFileAndOutputDatabase() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        //Retrieve DB file from resources folder
        URI dbSource = getDatabaseFileSourceUri(storageInterface);

        //Original query
        var originalSelect = Query.builder()
            .fetchType(Property.ofValue(FETCH))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .databaseUri(Property.ofValue(dbSource.toString()))
            .sql(Property.ofValue("SELECT * FROM new_table;"))
            .build().run(runContext);

        assertThat(originalSelect.getRows(), notNullValue());
        List<Integer> outputs = originalSelect.getRows().stream().map(m -> (int) m.get("i")).toList();
        assertThat(outputs, hasSize(3));
        assertThat(outputs.toArray(), arrayContainingInAnyOrder(1, 2, 3));

        //Insert data and output DB file
        var update = Query.builder()
            .fetchType(Property.ofValue(NONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .databaseUri(Property.ofValue(dbSource.toString()))
            .sql(Property.ofValue("INSERT  INTO new_table (i) VALUES (4);"))
            .outputDbFile(Property.ofValue(true))
            .build().run(runContext);

        //Check output
        var check = Query.builder()
            .fetchType(Property.ofValue(FETCH))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .databaseUri(Property.ofValue(update.getDatabaseUri().toString())) // use output db from last task
            .sql(Property.ofValue("SELECT * FROM new_table;"))
            .build().run(runContext);

        assertThat(check.getRows(), notNullValue());
        List<Integer> checkOutputs = check.getRows().stream().map(m -> (int) m.get("i")).toList();
        assertThat(checkOutputs, hasSize(4));
        assertThat(checkOutputs.toArray(), arrayContainingInAnyOrder(1, 2, 3, 4));
    }

    @Test
    void createLocalDbAndOutputDatabaseFile() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        //Create DB and Output file
        URI source = getCsvSourceUri(storageInterface);

        var createTable = Query.builder()
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .inputFiles(Map.of("in.csv", source.toString()))
            .outputFiles(Property.ofValue(List.of("out")))
            .sql(new Property<>("CREATE TABLE new_tbl AS SELECT * FROM read_csv_auto('in.csv', header=True);"))
            .outputDbFile(Property.ofValue(true))
            .build()
            .run(runContext);

        //Check table exists
        var getTable = Query.builder()
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .databaseUri(Property.ofValue(createTable.getDatabaseUri().toString()))
            .fetchType(Property.ofValue(FETCH))
            .sql(new Property<>("SELECT * FROM new_tbl;"))
            .build()
            .run(runContext);

        assertThat(getTable.getRows(), notNullValue());
        assertThat(getTable.getRows().size(), is(10));
        assertThat(getTable.getRows().getFirst().size(), is(12));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT [1, 2, 3] AS result",
        "SELECT array_value(1, 2, 3) AS result;"

    })
    void selectWithDuckDbArray(String sql) throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        var result = Query.builder()
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(new Property<>(sql))
            .fetchType(Property.ofValue(FETCH))
            .build()
            .run(runContext);

        assertThat(result.getRows(), notNullValue());
        assertThat(result.getRows().size(), is(1));
        Object[] resultInteger = (Object[]) result.getRows().getFirst().get("result");
        assertThat(resultInteger, arrayContainingInAnyOrder(1, 2, 3));
    }

    @Test
    void selectWithDuckDbArray_nestedArray() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        var result = Query.builder()
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(new Property<>("SELECT array_value(array_value(1, 2), array_value(3, 4)) as result;"))
            .fetchType(Property.ofValue(FETCH))
            .build()
            .run(runContext);

        assertThat(result.getRows(), notNullValue());
        assertThat(result.getRows().size(), is(1));
        Object[] resultInteger = (Object[]) result.getRows().getFirst().get("result");
        assertThat(resultInteger, is(notNullValue()));
        assertThat(resultInteger.length, is(2));

        Object[] firstArray = (Object[]) resultInteger[0];
        assertThat(firstArray, arrayContainingInAnyOrder(1, 2));

        Object[] secondArray = (Object[]) resultInteger[1];
        assertThat(secondArray, arrayContainingInAnyOrder(3, 4));
    }

    @Test
    void selectWithDuckDbArray_specialFunction_bid() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        var result = Query.builder()
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(new Property<>("""
                SELECT array_value(1, 2, 3);
                """))
            .fetchType(Property.ofValue(FETCH))
            .build()
            .run(runContext);

        assertThat(result.getRows(), notNullValue());
    }

    @ParameterizedTest
    @MethodSource("incorrectUrl")
    void urlNotCorrectFormat_shouldThrowException(Property<String> url) {
        RunContext runContext = runContextFactory.of(Map.of());

        Query task = Query.builder()
            .url(url)
            .fetchType(Property.ofValue(FETCH_ONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("SELECT array_value(1, 2, 3);"))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
    }

    public static Stream<Arguments> incorrectUrl() {
        return Stream.of(
            Arguments.of(new Property<>("")), //Empty URL
            Arguments.of(new Property<>("jdbc:postgresql://127.0.0.1:64790/kestra")) // Incorrect scheme
        );
    }
}
