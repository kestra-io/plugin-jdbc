package io.kestra.plugin.jdbc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Validates the production entry point wired into {@code AbstractJdbcQueries} / {@code AbstractJdbcQuery}:
 * it delegates to the source-preserving {@link SqlSplitter}, not {@link JooqSqlSplitter}. See
 * {@link JdbcStatementSplitter}'s Javadoc and {@link JooqSqlSplitterTest} for why.
 */
class JdbcStatementSplitterTest {

    @Test
    void delegatesToSqlSplitter() {
        String sql = """
            DROP TABLE IF EXISTS employee;
            CREATE TABLE employee(id INT, name VARCHAR(100));
            INSERT INTO employee(id, name) VALUES (1, 'John');
            """;

        assertArrayEquals(SqlSplitter.getQueries(sql), JdbcStatementSplitter.split(sql));
    }

    @Test
    void backtickQuotedIdentifiersAreNeverRoutedThroughJooq() {
        String sql = "SELECT `id;hello` FROM t; SELECT 2;";
        String[] queries = JdbcStatementSplitter.split(sql);

        assertEquals(2, queries.length);
        assertEquals("SELECT `id;hello` FROM t", queries[0]);
        assertEquals("SELECT 2", queries[1]);
    }

    @Test
    void sqlServerBracketQuotedIdentifiersAreNeverRoutedThroughJooq() {
        String sql = "SELECT [id;hello] FROM t; SELECT 2;";
        String[] queries = JdbcStatementSplitter.split(sql);

        assertEquals(2, queries.length);
        assertEquals("SELECT [id;hello] FROM t", queries[0]);
        assertEquals("SELECT 2", queries[1]);
    }

    @Test
    void oraclePlsqlBlockIsKeptAsOneStatementVerbatim() {
        String sql = """
            BEGIN
              BEGIN
                NULL;
              END;
              NULL;
            END;
            """;

        String[] queries = JdbcStatementSplitter.split(sql);

        assertEquals(1, queries.length);
        assertEquals(sql.trim(), queries[0]);
    }

    @Test
    void postgresDoDollarBlockIsKeptAsOneStatementVerbatim() {
        String sql = """
            DO $$
            BEGIN
                RAISE NOTICE 'hello; world';
            END
            $$;
            """;

        String[] queries = JdbcStatementSplitter.split(sql);

        assertEquals(1, queries.length);
        assertEquals(sql.trim(), queries[0]);
    }
}
