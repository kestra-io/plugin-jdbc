package io.kestra.plugin.jdbc.sqlite;

import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.jdbc.explorer.ExplorerTestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqliteExplorerTest {
    @TempDir
    static Path dir;
    static Path database;

    @BeforeAll
    static void createDatabase() throws Exception {
        database = dir.resolve("explorer.sqlite");
        try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database); var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE people (id INTEGER, name TEXT)");
            statement.execute("CREATE TABLE t (a INTEGER, b INTEGER)");
            statement.execute("CREATE TABLE tt (a INTEGER, b INTEGER)");
            statement.execute("INSERT INTO people VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            statement.execute("CREATE VIEW adults AS SELECT * FROM people WHERE id > 1");
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> run(SqliteQueryEndpoint endpoint, Map<String, Object> body) throws Exception {
        var response = endpoint.handle(ExplorerTestContext.of(database, body));
        var result = JacksonMapper.ofJson().readValue(response.body(), Map.class);
        if (result.containsKey("error")) {
            throw new IllegalArgumentException((String) result.get("error"));
        }
        return result;
    }

    @Test
    void catalogListsTablesViewsAndColumns() throws Exception {
        var response = new SqliteCatalogEndpoint().handle(ExplorerTestContext.of(database, Map.of()));
        var catalog = JacksonMapper.ofJson().readTree(response.body());

        assertThat(new SqliteCatalogEndpoint().name(), is("sqlite-catalog"));
        assertThat(catalog.get("tables").size(), is(4));
        assertThat(catalog.get("tables").get(1).get("name").asText(), is("people"));
        assertThat(catalog.get("tables").get(1).get("columns").get(1).get("name").asText(), is("name"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void queryPages() throws Exception {
        var result = run(new SqliteQueryEndpoint(), Map.of("sql", "SELECT * FROM people ORDER BY id", "limit", 2));

        assertThat((List<Object>) result.get("rows"), hasSize(2));
        assertThat(result.get("hasMore"), is(true));
    }

    @Test
    @SuppressWarnings("unchecked")
    void tableInfoAndWithAreAllowed() throws Exception {
        assertThat(
            (List<Object>) run(new SqliteQueryEndpoint(), Map.of("sql", "WITH t AS (SELECT 1 AS a) SELECT * FROM t")).get("rows"),
            hasSize(1)
        );
        assertThat(
            (List<Object>) run(new SqliteQueryEndpoint(), Map.of("sql", "PRAGMA table_info('people')")).get("rows"),
            hasSize(2)
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "INSERT INTO people VALUES (9, 'z')",
        "CREATE TABLE t (a INT)",
        "DROP TABLE people",
        "UPDATE people SET id = 1",
        "DELETE FROM people",
        "ATTACH DATABASE '/tmp/other.db' AS other",
        "DETACH DATABASE main",
        "SELECT load_extension('/tmp/evil.so')",
        "VACUUM",
        "PRAGMA writable_schema = ON",
        "PRAGMA query_only = OFF",
        "SELECT * FROM people; DROP TABLE people",
        "WITH t AS (SELECT 1) INSERT INTO people SELECT * FROM t",
        "EXPLAIN PRAGMA query_only=OFF",
        "EXPLAIN QUERY PLAN PRAGMA temp_store_directory='/tmp'",
        "EXPLAIN INSERT INTO people VALUES (9, 'z')",
        "EXPLAIN",
    })
    void writesAndUnsafeStatementsAreRefused(String sql) {
        assertThrows(IllegalArgumentException.class, () -> run(new SqliteQueryEndpoint(), Map.of("sql", sql)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"EXPLAIN SELECT * FROM people", "explain query plan SELECT * FROM people", "EXPLAIN WITH t AS (SELECT 1) SELECT * FROM t"})
    void explainOverReadStatementsIsAllowed(String sql) throws Exception {
        assertThat(run(new SqliteQueryEndpoint(), Map.of("sql", sql)).get("columns"), notNullValue());
    }

    @Test
    void filesThatAreNotSqliteAreRefusedBeforeTheDriverOpensThem() throws Exception {
        var fake = dir.resolve("fake.sqlite");
        java.nio.file.Files.writeString(fake, "this is not a database, only text of some length");

        var response = new SqliteQueryEndpoint().handle(ExplorerTestContext.of(fake, Map.of("sql", "SELECT 1")));
        var result = JacksonMapper.ofJson().readValue(response.body(), Map.class);

        assertThat((String) result.get("error"), containsString("not a SQLite database"));
    }

    @Test
    void keywordsInsideStringsAreNotRefused() throws Exception {
        var result = run(new SqliteQueryEndpoint(), Map.of("sql", "SELECT 'DROP TABLE; insert' AS text"));

        assertThat(result.get("hasMore"), is(false));
    }

    @Test
    void errorsNeverContainFilePaths() {
        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> run(new SqliteQueryEndpoint(), Map.of("sql", "SELECT * FROM missing_table"))
        );

        assertThat(exception.getMessage(), not(containsString(dir.toString())));
    }

    @Test
    void runawayQueryIsCancelledByTheTimeout() {
        var endpoint = new SqliteQueryEndpoint() {
            @Override
            protected Duration timeout() {
                return Duration.ofSeconds(1);
            }
        };
        var runaway = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c";

        var startedAt = System.nanoTime();
        assertThrows(IllegalArgumentException.class, () -> run(endpoint, Map.of("sql", runaway)));

        assertThat(Duration.ofNanos(System.nanoTime() - startedAt).toSeconds(), lessThan(20L));
    }

    @Test
    void hugeOffsetIsCapped() throws Exception {
        var result = run(new SqliteQueryEndpoint(), Map.of("sql", "SELECT 1 AS one", "offset", 2147483647));

        assertThat(((List<?>) result.get("rows")).size(), is(0));
    }

    @Test
    void divisionInErrorsIsNotMangled() {
        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> run(new SqliteQueryEndpoint(), Map.of("sql", "SELECT a/b FROM missing_table"))
        );

        assertThat(exception.getMessage(), not(containsString("<path>")));
    }

    @Test
    void worksWhenNoDriverIsRegisteredWithDriverManager() throws Exception {
        var registered = Collections.list(DriverManager.getDrivers());
        for (var driver : registered) {
            DriverManager.deregisterDriver(driver);
        }
        try {
            var result = run(new SqliteQueryEndpoint(), Map.of("sql", "SELECT 1 AS one"));
            assertThat(((List<?>) result.get("rows")).size(), is(1));
        } finally {
            for (var driver : registered) {
                DriverManager.registerDriver(driver);
            }
        }
    }

    // The statements the explorer UI sends for its column cards (see ui/src/lib/explorer.ts).
    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT count(*) AS total FROM (SELECT * FROM people) q",
        "SELECT count(*) AS count, count(\"id\") AS non_null, count(DISTINCT \"id\") AS distinct_values, min(\"id\") AS min, max(\"id\") AS max, avg(\"id\") AS avg FROM (SELECT * FROM people) q",
        "SELECT CAST(CASE WHEN s.mx = s.mn THEN 0 ELSE (v.x - s.mn) * 10.0 / (s.mx - s.mn) END AS INTEGER) AS bucket, count(*) AS n FROM (SELECT CAST(\"id\" AS REAL) AS x FROM (SELECT * FROM people) q) v, (SELECT min(x) AS mn, max(x) AS mx FROM (SELECT CAST(\"id\" AS REAL) AS x FROM (SELECT * FROM people) q)) s WHERE v.x IS NOT NULL GROUP BY bucket ORDER BY bucket",
        "SELECT \"name\" AS value, count(*) AS n FROM (SELECT * FROM people) q WHERE \"name\" IS NOT NULL GROUP BY \"name\" ORDER BY n DESC LIMIT 3"
    })
    void columnExplorerStatementsRun(String sql) throws Exception {
        assertThat(run(new SqliteQueryEndpoint(), Map.of("sql", sql)).get("columns"), notNullValue());
    }

    @ParameterizedTest
    @ValueSource(strings = {"PRAGMA table_info(people)", "PRAGMA table_info('people')", "PRAGMA table_info(\"people\")", "pragma table_info( people );", "PRAGMA table_info(t)", "PRAGMA table_info(tt)", "pragma table_info( t );"})
    @SuppressWarnings("unchecked")
    void tableInfoAcceptsQuotedAndUnquotedIdentifiers(String sql) throws Exception {
        assertThat((List<Object>) run(new SqliteQueryEndpoint(), Map.of("sql", sql)).get("rows"), hasSize(2));
    }

    @ParameterizedTest
    @ValueSource(strings = {"PRAGMA table_info(people, other)", "PRAGMA table_info(people); PRAGMA user_version", "PRAGMA table_info"})
    void otherPragmaFormsAreRefused(String sql) {
        var exception = assertThrows(IllegalArgumentException.class, () -> run(new SqliteQueryEndpoint(), Map.of("sql", sql)));
        assertThat(exception.getMessage(), anyOf(containsString("PRAGMA table_info(<table>)"), containsString("single read-only")));
    }

    @Test
    @SuppressWarnings("unchecked")
    void valuesAboveTheLengthLimitAreRefusedAndLargeOnesCut() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> run(new SqliteQueryEndpoint(), Map.of("sql", "SELECT printf('%.*c', 900000000, 'x')")));

        var result = run(new SqliteQueryEndpoint(), Map.of("sql", "SELECT printf('%.*c', 500000, 'x') AS big"));
        assertThat(((String) ((List<List<Object>>) result.get("rows")).getFirst().getFirst()).length(), lessThanOrEqualTo(260));
    }
}
