package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Files;
import java.sql.DriverManager;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertThrows;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/** Compatibility coverage for the DuckDB 2.0 upgrade. */
@KestraTest
class DuckDbUpgradeCompatibilityTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @ParameterizedTest
    @ValueSource(strings = {"1.4.5", "1.5.6"})
    void legacyDatabaseCanBeReadModifiedAndExported(String version) throws Exception {
        URI source;
        try (var input = Objects.requireNonNull(getClass().getResourceAsStream("/db/duckdb-" + version + ".db"))) {
            source = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create()), input);
        }

        var before = Query.builder()
            .databaseUri(Property.ofValue(source.toString()))
            .communityExtensions(Property.ofValue(List.of()))
            .sql(Property.ofValue("SELECT created_by, (SELECT count(*)::INTEGER FROM legacy_events) AS n FROM fixture_metadata"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(runContextFactory.of(Map.of()));
        assertThat(before.getRow(), is(Map.of("created_by", "v" + version, "n", 2)));

        var modified = Query.builder()
            .databaseUri(Property.ofValue(source.toString()))
            .communityExtensions(Property.ofValue(List.of()))
            .sql(Property.ofValue("INSERT INTO legacy_events VALUES (3, 'after upgrade')"))
            .fetchType(Property.ofValue(FetchType.NONE))
            .outputDbFile(Property.ofValue(true))
            .build().run(runContextFactory.of(Map.of()));
        assertThat(modified.getDatabaseUri(), notNullValue());

        var reopened = Query.builder()
            .databaseUri(Property.ofValue(modified.getDatabaseUri().toString()))
            .communityExtensions(Property.ofValue(List.of()))
            .sql(Property.ofValue("SELECT id, label FROM legacy_events ORDER BY id"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build().run(runContextFactory.of(Map.of()));
        assertThat(reopened.getRows(), is(List.of(
            Map.of("id", 1, "label", "before upgrade"),
            Map.of("id", 2, "label", "retained"),
            Map.of("id", 3, "label", "after upgrade")
        )));

        // The task works on a copy: its input must still contain only the original rows.
        var original = Query.builder()
            .databaseUri(Property.ofValue(source.toString()))
            .communityExtensions(Property.ofValue(List.of()))
            .sql(Property.ofValue("SELECT count(*)::INTEGER AS n FROM legacy_events"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(runContextFactory.of(Map.of()));
        assertThat(original.getRow().get("n"), is(2));
    }

    @Test
    void recoversCommittedRowsFromLegacyWal(@TempDir Path directory) throws Exception {
        Path database = directory.resolve("recovered.db");
        for (String suffix : List.of("", ".wal")) {
            try (var input = Objects.requireNonNull(getClass().getResourceAsStream("/db/duckdb-1.5.6-wal.db" + suffix))) {
                Files.copy(input, Path.of(database + suffix));
            }
        }
        assertThat(Files.size(Path.of(database + ".wal")), greaterThan(0L));
        var recovered = Query.builder()
            .url(Property.ofValue("jdbc:duckdb:" + database))
            .communityExtensions(Property.ofValue(List.of()))
            .sql(Property.ofValue("SELECT id, label FROM legacy_events ORDER BY id"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .outputDbFile(Property.ofValue(true))
            .build().run(runContextFactory.of(Map.of()));
        assertThat(recovered.getRows(), is(List.of(
            Map.of("id", 1, "label", "before upgrade"),
            Map.of("id", 2, "label", "retained"),
            Map.of("id", 3, "label", "recovered from WAL")
        )));
        var reopened = Query.builder()
            .databaseUri(Property.ofValue(recovered.getDatabaseUri().toString()))
            .communityExtensions(Property.ofValue(List.of()))
            .sql(Property.ofValue("SELECT label FROM legacy_events WHERE id = 3"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(runContextFactory.of(Map.of()));
        assertThat(reopened.getRow(), is(Map.of("label", "recovered from WAL")));
    }

    @ParameterizedTest
    @EnumSource(value = FetchType.class, names = {"FETCH", "FETCH_ONE", "STORE"})
    void namedParametersCastsQuotedIdentifiersAndJsonSurviveOutputModes(FetchType fetchType) throws Exception {
        var output = Query.builder()
            .communityExtensions(Property.ofValue(List.of()))
            .parameters(Property.ofValue(Map.of("value", 7)))
            .sql(Property.ofValue("""
                SELECT :value::INTEGER AS "Quoted:Value", '10:30' AS literal,
                       '{"tags":["a","b"],"ok":true}'::JSON AS payload
                """))
            .fetchType(Property.ofValue(fetchType))
            .build().run(runContextFactory.of(Map.of()));

        var expected = Map.of(
            "Quoted:Value", 7, "literal", "10:30",
            "payload", Map.of("tags", List.of("a", "b"), "ok", true)
        );
        switch (fetchType) {
            case FETCH_ONE -> assertThat(output.getRow(), is(expected));
            case FETCH -> assertThat(output.getRows(), is(List.of(expected)));
            case STORE -> {
                assertThat(output.getUri(), notNullValue());
                try (var input = storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri())) {
                    var rows = FileSerde.readAll(input).collectList().block();
                    assertThat(rows, is(List.of(expected)));
                }
            }
            default -> throw new IllegalArgumentException("Unexpected fetch type " + fetchType);
        }
    }

    @Test
    void namedTimeZonesAndTimestampWithTimeZoneAreConverted() throws Exception {
        var output = Query.builder()
            .communityExtensions(Property.ofValue(List.of()))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("""
                SELECT TIMESTAMPTZ '2024-07-01 12:34:56.123456+02:00' AS instant,
                       TIMESTAMPTZ '2024-07-01 12:34:56.123456+02:00'
                           AT TIME ZONE 'America/New_York' AS local_time
                """))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(runContextFactory.of(Map.of()));
        assertThat(output.getRow().get("instant"), is(OffsetDateTime.parse("2024-07-01T10:34:56.123456Z")));
        assertThat(output.getRow().get("local_time"), is(ZonedDateTime.parse("2024-07-01T06:34:56.123456+02:00[Europe/Paris]")));
    }

    @ParameterizedTest
    @EnumSource(value = FetchType.class, names = {"FETCH", "FETCH_ONE", "STORE"})
    void variantsAreMaterializedAsStructuredValues(FetchType fetchType) throws Exception {
        var output = Query.builder()
            .communityExtensions(Property.ofValue(List.of()))
            .sql(Property.ofValue("""
                SELECT {'id': 1, 'tags': ['a', 'b'],
                        'nested': {'text': 'O''Brien: {quoted}', 'missing': NULL}}::VARIANT AS payload,
                       [1, NULL, 3]::VARIANT AS items,
                       42::VARIANT AS number, true::VARIANT AS flag,
                       'plain text'::VARIANT AS text, NULL::VARIANT AS missing,
                       '{"id":1,"large":2147483648,"huge":18446744073709551615}'::JSON::VARIANT AS json_numbers
                """))
            .fetchType(Property.ofValue(fetchType))
            .build().run(runContextFactory.of(Map.of()));

        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("text", "O'Brien: {quoted}");
        nested.put("missing", null);
        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("payload", Map.of("id", 1, "tags", List.of("a", "b"), "nested", nested));
        expected.put("items", Arrays.asList(1, null, 3));
        expected.put("number", 42);
        expected.put("flag", true);
        expected.put("text", "plain text");
        expected.put("missing", null);
        expected.put("json_numbers", Map.of("id", 1, "large", 2147483648L,
            "huge", new java.math.BigInteger("18446744073709551615")));

        switch (fetchType) {
            case FETCH_ONE -> assertThat(output.getRow(), is(expected));
            case FETCH -> assertThat(output.getRows(), is(List.of(expected)));
            case STORE -> {
                try (var input = storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri())) {
                    assertThat(FileSerde.readAll(input).collectList().block(), is(List.of(expected)));
                }
            }
            default -> throw new IllegalArgumentException("Unexpected fetch type " + fetchType);
        }
    }

    @Test
    void migrationSqlChangesAndWorkaroundsAreExplicit() throws Exception {
        try (var connection = DriverManager.getConnection("jdbc:duckdb:");
             var statement = connection.createStatement()) {
            assertThrows(java.sql.SQLException.class, () -> execute(connection, "SELECT 1 // 0"));
            assertThrows(java.sql.SQLException.class, () -> execute(connection, "SELECT 1 % 0"));
            try (var result = statement.executeQuery("SELECT TRY(1 // 0), TRY(1 % 0), 'abc' ~ 'b'")) {
                assertThat(result.next(), is(true));
                assertThat(result.getObject(1), nullValue());
                assertThat(result.getObject(2), nullValue());
                assertThat(result.getBoolean(3), is(true));
            }
            statement.execute("SET error_on_division_by_zero=false");
            try (var result = statement.executeQuery("SELECT 1 // 0, 1 % 0")) {
                assertThat(result.next(), is(true));
                assertThat(result.getObject(1), nullValue());
                assertThat(result.getObject(2), nullValue());
            }
            assertThrows(java.sql.SQLException.class,
                () -> execute(connection, "SELECT list_transform([1,2], x -> x + 1)"));
            statement.execute("SET lambda_syntax='enable_single_arrow'");
            try (var result = statement.executeQuery("SELECT list_transform([1,2], x -> x + 1)")) {
                assertThat(result.next(), is(true));
                assertThat((Object[]) result.getArray(1).getArray(), arrayContaining(2, 3));
            }
        }
    }

    @Test
    void explicitTransactionErrorsRequireRollback() throws Exception {
        try (var connection = DriverManager.getConnection("jdbc:duckdb:");
             var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE transaction_events(id INTEGER)");
            connection.setAutoCommit(false);
            assertThrows(java.sql.SQLException.class, () -> execute(connection, "SELECT CAST('bad' AS INTEGER)"));
            assertThrows(java.sql.SQLException.class, () -> execute(connection, "INSERT INTO transaction_events VALUES (1)"));
            connection.rollback();
            statement.execute("INSERT INTO transaction_events VALUES (2)");
            connection.commit();
            try (var result = statement.executeQuery("SELECT id FROM transaction_events")) {
                assertThat(result.next(), is(true));
                assertThat(result.getInt(1), is(2));
                assertThat(result.next(), is(false));
            }
        }
    }

    private static void execute(java.sql.Connection connection, String sql) throws java.sql.SQLException {
        // DuckDB closes a statement after an execution error. Use a fresh one
        // so subsequent assertions check connection/transaction behavior.
        try (var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    @Test
    void triggerPollsConfiguredDatabaseAndRunsAfterSql(@TempDir Path directory) throws Exception {
        String url = "jdbc:duckdb:" + directory.resolve("events.db");
        try (var connection = DriverManager.getConnection(url);
             var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE events AS SELECT 1 AS id, false AS processed");
        }
        var trigger = Trigger.builder()
            .url(Property.ofExpression("{{ vars.jdbc_url }}"))
            .parameters(Property.ofValue(Map.of("id", 1)))
            .communityExtensions(Property.ofValue(List.of()))
            .sql(Property.ofValue("SELECT id FROM events WHERE id = :id AND NOT processed"))
            .afterSQL(Property.ofValue("UPDATE events SET processed = true WHERE id = :id"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();
        var first = trigger.runQuery(runContextFactory.of(Map.of("vars", Map.of("jdbc_url", url))));
        assertThat(first.getRows(), is(List.of(Map.of("id", 1))));
        var second = trigger.runQuery(runContextFactory.of(Map.of("vars", Map.of("jdbc_url", url))));
        assertThat(second.getRows(), empty());
    }

    @Test
    void queryAndQueriesKeepExtensionFilesInTheirTaskDirectory() throws Exception {
        String sql = """
            SELECT current_setting('extension_repository_directory') AS repositories,
                   current_setting('extension_directory') AS extensions,
                   current_setting('allow_unsigned_extensions') AS unsigned
            """;
        var queryContext = runContextFactory.of(Map.of());
        var query = Query.builder()
            .communityExtensions(Property.ofValue(List.of()))
            .sql(Property.ofValue(sql))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(queryContext);
        assertThat(Path.of(query.getRow().get("repositories").toString()).startsWith(queryContext.workingDir().path()), is(true));
        assertThat(Path.of(query.getRow().get("extensions").toString()).startsWith(queryContext.workingDir().path()), is(true));
        assertThat(query.getRow().get("unsigned"), is(false));

        var queriesContext = runContextFactory.of(Map.of());
        var queries = Queries.builder()
            .communityExtensions(Property.ofValue(List.of()))
            .sql(Property.ofValue(sql))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(queriesContext);
        var row = queries.getOutputs().getFirst().getRow();
        assertThat(Path.of(row.get("repositories").toString()).startsWith(queriesContext.workingDir().path()), is(true));
        assertThat(Path.of(row.get("extensions").toString()).startsWith(queriesContext.workingDir().path()), is(true));
        assertThat(row.get("unsigned"), is(false));
    }

    @Test
    void ionToParquetAndDatabaseRoundTrip() throws Exception {
        var output = Queries.builder()
            .inputFiles(Map.of("upgrade.ion", "{id: 1, label: \"before\"}\n{id: 2, label: \"after\"}\n"))
            .sql(Property.ofExpression("""
                CREATE TABLE events AS
                    SELECT * FROM read_ion('upgrade.ion', columns := {id: 'INTEGER', label: 'VARCHAR'});
                COPY events TO '{{ workingDir }}/events.parquet' (FORMAT PARQUET);
                SELECT id, label FROM events ORDER BY id;
                """))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .outputDbFile(Property.ofValue(true))
            .build().run(runContextFactory.of(Map.of()));

        var expected = List.of(Map.of("id", 1, "label", "before"), Map.of("id", 2, "label", "after"));
        assertThat(output.getOutputs().getLast().getRows(), is(expected));
        assertThat(output.getOutputFiles().get("events.parquet"), notNullValue());
        assertThat(output.getDatabaseUri(), notNullValue());

        var reopened = Query.builder()
            .databaseUri(Property.ofValue(output.getDatabaseUri().toString()))
            .inputFiles(Map.of("events.parquet", output.getOutputFiles().get("events.parquet").toString()))
            .communityExtensions(Property.ofValue(List.of()))
            .sql(Property.ofValue("""
                SELECT e.id, p.label FROM events e
                JOIN read_parquet('events.parquet') p USING (id) ORDER BY e.id
                """))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build().run(runContextFactory.of(Map.of()));
        assertThat(reopened.getRows(), is(expected));
    }

    @Test
    void queriesSupportVariantsAndNewLambdaSyntax() throws Exception {
        var output = Queries.builder()
            .communityExtensions(Property.ofValue(List.of()))
            .parameters(Property.ofValue(Map.of("id", 7)))
            .sql(Property.ofValue("""
                CREATE TABLE events AS SELECT {'id': :id::INTEGER}::VARIANT AS payload;
                SELECT payload FROM events;
                SELECT list_transform([1, 2], lambda x : x + 1)::VARIANT AS incremented;
                """))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(runContextFactory.of(Map.of()));
        assertThat(output.getOutputs(), hasSize(2));
        assertThat(output.getOutputs().getFirst().getRow(), is(Map.of("payload", Map.of("id", 7))));
        assertThat(output.getOutputs().getLast().getRow(), is(Map.of("incremented", List.of(2, 3))));
    }

    @Test
    void jdbcSupportsMemoryFileAndReadOnlyConnections(@TempDir Path directory) throws Exception {
        String url = "jdbc:duckdb:" + directory.resolve("modes.duckdb");
        var properties = new Properties();
        properties.setProperty("extension_directory", directory.resolve("extensions").toString());
        properties.setProperty("preserve_identifier_case", "true");
        try (var connection = DriverManager.getConnection(url, properties);
             var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE events AS SELECT 1 AS id");
        }
        properties.setProperty("duckdb.read_only", "true");
        try (var connection = DriverManager.getConnection(url, properties);
             var statement = connection.createStatement();
             var result = statement.executeQuery("SELECT id FROM events")) {
            assertThat(result.next(), is(true));
            assertThat(result.getInt(1), is(1));
            assertThrows(java.sql.SQLException.class, () -> statement.execute("INSERT INTO events VALUES (2)"));
        }
    }

    // Task parameters expose :name bindings. Native JDBC positional bindings are checked separately.
    @ParameterizedTest
    @ValueSource(strings = {"?", "$1"})
    void jdbcPositionalParametersAcceptCastsAndQuotedIdentifiers(String placeholder) throws Exception {
        try (var connection = DriverManager.getConnection("jdbc:duckdb:");
             var statement = connection.prepareStatement("SELECT " + placeholder + "::INTEGER AS \"Quoted:Value\"")) {
            statement.setObject(1, 7);
            try (var result = statement.executeQuery()) {
                assertThat(result.next(), is(true));
                assertThat(result.getInt("Quoted:Value"), is(7));
                assertThat(result.next(), is(false));
            }
        }
    }
}
