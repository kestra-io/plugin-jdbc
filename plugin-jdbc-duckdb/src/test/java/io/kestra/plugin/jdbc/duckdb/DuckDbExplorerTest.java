package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.jdbc.explorer.ExplorerTestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DuckDbExplorerTest {
    @TempDir
    static Path dir;
    static Path database;

    @BeforeAll
    static void createDatabase() throws Exception {
        database = dir.resolve("explorer.duckdb");
        try (var connection = DriverManager.getConnection("jdbc:duckdb:" + database); var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE people (id INTEGER, name VARCHAR)");
            statement.execute("INSERT INTO people SELECT i, repeat('x', 300) FROM range(5) t(i)");
            statement.execute("CREATE VIEW adults AS SELECT * FROM people WHERE id > 1");
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> run(DuckDbQueryEndpoint endpoint, Map<String, Object> body) throws Exception {
        var response = endpoint.handle(ExplorerTestContext.of(database, body));
        var result = JacksonMapper.ofJson().readValue(response.body(), Map.class);
        if (result.containsKey("error")) {
            throw new IllegalArgumentException((String) result.get("error"));
        }
        return result;
    }

    @Test
    void catalogListsTablesViewsAndColumns() throws Exception {
        var response = new DuckDbCatalogEndpoint().handle(ExplorerTestContext.of(database, Map.of()));
        var catalog = JacksonMapper.ofJson().readTree(response.body());

        assertThat(new DuckDbCatalogEndpoint().name(), is("duckdb-catalog"));
        assertThat(catalog.get("tables").size(), is(2));
        assertThat(catalog.get("tables").get(0).get("name").asText(), is("adults"));
        assertThat(catalog.get("tables").get(0).get("type").asText(), is("VIEW"));
        assertThat(catalog.get("tables").get(1).get("name").asText(), is("people"));
        assertThat(catalog.get("tables").get(1).get("columns").size(), is(2));
    }

    @Test
    @SuppressWarnings("unchecked")
    void queryPagesAndCutsCells() throws Exception {
        var result = run(new DuckDbQueryEndpoint(), Map.of("sql", "SELECT * FROM people ORDER BY id", "limit", 2, "offset", 1));

        var rows = (java.util.List<java.util.List<Object>>) result.get("rows");
        assertThat(rows, hasSize(2));
        assertThat(rows.getFirst().getFirst(), is(1));
        assertThat(((String) rows.getFirst().get(1)).length(), lessThanOrEqualTo(259));
        assertThat(result.get("hasMore"), is(true));
        assertThat(((java.util.List<Object>) result.get("columns")), hasSize(2));
        assertThat(result.get("durationMs"), notNullValue());
    }

    @Test
    void limitIsCappedAtOneThousand() throws Exception {
        var result = run(new DuckDbQueryEndpoint(), Map.of("sql", "SELECT * FROM range(5000)", "limit", 50000));

        assertThat(((java.util.List<?>) result.get("rows")).size(), is(1000));
        assertThat(result.get("hasMore"), is(true));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "INSERT INTO people VALUES (9, 'z')",
        "CREATE TABLE t (a INT)",
        "DROP TABLE people",
        "ATTACH '/tmp/other.db' AS other",
        "COPY people TO '/tmp/out.csv'",
        "INSTALL httpfs",
        "LOAD httpfs",
        "SELECT * FROM read_csv('/etc/passwd')",
        "SELECT * FROM people; DROP TABLE people",
        "SET enable_external_access=true",
        "PRAGMA database_list",
        "UPDATE people SET id = 1",
    })
    void writesAndFileAccessAreRefused(String sql) {
        assertThrows(IllegalArgumentException.class, () -> run(new DuckDbQueryEndpoint(), Map.of("sql", sql)));
    }

    @Test
    void errorsNeverContainFilePaths() {
        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> run(new DuckDbQueryEndpoint(), Map.of("sql", "SELECT * FROM read_csv('/etc/passwd')"))
        );

        assertThat(exception.getMessage(), not(containsString("/etc/passwd")));
        assertThat(exception.getMessage(), not(containsString(dir.toString())));
    }

    @Test
    void runawayQueryIsCancelledByTheTimeout() throws Exception {
        var endpoint = new DuckDbQueryEndpoint() {
            @Override
            protected Duration timeout() {
                return Duration.ofSeconds(1);
            }
        };

        var startedAt = System.nanoTime();
        assertThrows(
            IllegalArgumentException.class,
            () -> run(endpoint, Map.of("sql", "SELECT count(*) FROM range(1000000000000) t(i) WHERE i % 7 = 3"))
        );

        assertThat(Duration.ofNanos(System.nanoTime() - startedAt).toSeconds(), lessThan(20L));
    }

    @Test
    void missingDatabaseUriGivesActionableError() {
        var response = new DuckDbQueryEndpoint().handle(new ExplorerTestContext.NoOutputs().context(Map.of("sql", "SELECT 1")));

        assertThat(new String(response.body()), containsString("outputDbFile: true"));
    }

    @Test
    void hugeOffsetIsCapped() throws Exception {
        var result = run(new DuckDbQueryEndpoint(), Map.of("sql", "SELECT 1 AS one", "offset", 2147483647));

        assertThat(((java.util.List<?>) result.get("rows")).size(), is(0));
    }

    @Test
    void divisionInErrorsIsNotMangled() {
        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> run(new DuckDbQueryEndpoint(), Map.of("sql", "SELECT a/b FROM missing_table"))
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
            var result = run(new DuckDbQueryEndpoint(), Map.of("sql", "SELECT 1 AS one"));
            assertThat(((java.util.List<?>) result.get("rows")).size(), is(1));
        } finally {
            for (var driver : registered) {
                java.sql.DriverManager.registerDriver(driver);
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void describeAndSummarizeAreAllowedAsSubqueries() throws Exception {
        var describe = run(new DuckDbQueryEndpoint(), Map.of("sql", "SELECT * FROM (DESCRIBE people)"));
        var summarize = run(new DuckDbQueryEndpoint(), Map.of("sql", "SELECT * FROM (SUMMARIZE SELECT id FROM (SELECT * FROM people) q)"));

        assertThat((java.util.List<Object>) describe.get("rows"), hasSize(2));
        assertThat((java.util.List<Object>) summarize.get("rows"), hasSize(1));
    }

    // The statements the explorer UI sends for its column cards (see ui/src/lib/explorer.ts).
    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT count(*) AS total FROM (SELECT * FROM people) q",
        "SELECT * FROM (SUMMARIZE SELECT \"id\" FROM (SELECT * FROM people) q)",
        "SELECT CAST(floor(CASE WHEN s.mx = s.mn THEN 0 ELSE (v.x - s.mn) * 10.0 / (s.mx - s.mn) END) AS INTEGER) AS bucket, count(*) AS n FROM (SELECT CAST(\"id\" AS DOUBLE) AS x FROM (SELECT * FROM people) q) v, (SELECT min(x) AS mn, max(x) AS mx FROM (SELECT CAST(\"id\" AS DOUBLE) AS x FROM (SELECT * FROM people) q)) s WHERE v.x IS NOT NULL GROUP BY bucket ORDER BY bucket",
        "SELECT \"name\" AS value, count(*) AS n FROM (SELECT * FROM people) q WHERE \"name\" IS NOT NULL GROUP BY \"name\" ORDER BY n DESC LIMIT 3"
    })
    void columnExplorerStatementsRun(String sql) throws Exception {
        assertThat(run(new DuckDbQueryEndpoint(), Map.of("sql", sql)).get("columns"), notNullValue());
    }
}
