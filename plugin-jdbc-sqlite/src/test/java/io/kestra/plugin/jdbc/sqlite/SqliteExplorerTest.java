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
        assertThat(catalog.get("tables").size(), is(2));
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
    })
    void writesAndUnsafeStatementsAreRefused(String sql) {
        assertThrows(IllegalArgumentException.class, () -> run(new SqliteQueryEndpoint(), Map.of("sql", sql)));
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

        assertThat(((java.util.List<?>) result.get("rows")).size(), is(0));
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
        var registered = java.util.Collections.list(java.sql.DriverManager.getDrivers());
        for (var driver : registered) {
            java.sql.DriverManager.deregisterDriver(driver);
        }
        try {
            var result = run(new SqliteQueryEndpoint(), Map.of("sql", "SELECT 1 AS one"));
            assertThat(((java.util.List<?>) result.get("rows")).size(), is(1));
        } finally {
            for (var driver : registered) {
                java.sql.DriverManager.registerDriver(driver);
            }
        }
    }
}
