package io.kestra.plugin.jdbc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlSplitterTest {

    @Test
    void simpleSelect() {
        String sql = "SELECT 1;";
        String[] queries = SqlSplitter.getQueries(sql);

        assertEquals(1, queries.length);
        assertEquals("SELECT 1", queries[0]);
    }

    @Test
    void multipleSelects() {
        String sql = "SELECT 1; SELECT 2; SELECT 3;";
        String[] queries = SqlSplitter.getQueries(sql);

        assertEquals(3, queries.length);
        assertEquals("SELECT 1", queries[0]);
        assertEquals("SELECT 2", queries[1]);
        assertEquals("SELECT 3", queries[2]);
    }

    @Test
    void multipleDdlAndDml() {
        String sql = """
            DROP TABLE IF EXISTS employee;
            CREATE TABLE employee(id INT, name VARCHAR(100));
            INSERT INTO employee(id, name) VALUES (1, 'John');
            """;

        String[] queries = SqlSplitter.getQueries(sql);

        assertEquals(3, queries.length);
        assertEquals("DROP TABLE IF EXISTS employee", queries[0]);
        assertEquals("CREATE TABLE employee(id INT, name VARCHAR(100))", queries[1]);
        assertEquals("INSERT INTO employee(id, name) VALUES (1, 'John')", queries[2]);
    }

    @Test
    void doubleDollarQuerySimple() {
        // Simple case: $$...$$ with a single ; at the end
        String sql = "$$SELECT 1$$;";
        String[] queries = SqlSplitter.getQueries(sql);

        assertEquals(1, queries.length);
        assertEquals("$$SELECT 1$$", queries[0]);
    }

    @Test
    void postgresDoDollarBlock() {
        String sql = """
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'categories') THEN
                    CREATE TABLE categories (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(40)
                    );

                    INSERT INTO categories (name) VALUES ('Games');
                    INSERT INTO categories (name) VALUES ('Multimedia');
                    INSERT INTO categories (name) VALUES ('Productivity');
                    INSERT INTO categories (name) VALUES ('Tools');
                    INSERT INTO categories (name) VALUES ('Health');
                    INSERT INTO categories (name) VALUES ('Lifestyle');
                    INSERT INTO categories (name) VALUES ('Other');
                END IF;
            END
            $$;
            """;

        String[] queries = SqlSplitter.getQueries(sql);

        assertEquals(1, queries.length);
        assertEquals(sql.trim(), queries[0]);
    }

    @Test
    void plsqlBlockSingleStatement() {
        String plsql = """
            BEGIN
              FOR record IN (
                SELECT ROWNUM n
                FROM ( SELECT 1 just_a_column
                  FROM dual
                  GROUP BY CUBE(1,2,3,4,5,6,7,8,9) )
                  WHERE ROWNUM <= 20
                )
                LOOP
                  dbms_output.put_line(record.n);
                END LOOP;
            END;
            """;

        String[] queries = SqlSplitter.getQueries(plsql);

        assertEquals(1, queries.length);
        assertEquals(plsql.trim(), queries[0]);
    }

    @Test
    void transactionLikeBeginEndWithOperations() {
        String sql = """
            BEGIN
                INSERT INTO tx_oracle_ops (id) VALUES (1);
                INSERT INTO tx_oracle_ops (id) VALUES (2);
            END;
            SELECT COUNT(*) AS CNT FROM tx_oracle_ops;
            """;

        String[] queries = SqlSplitter.getQueries(sql);

        assertEquals(2, queries.length);
        assertEquals("""
            BEGIN
                INSERT INTO tx_oracle_ops (id) VALUES (1);
                INSERT INTO tx_oracle_ops (id) VALUES (2);
            END;
            """.trim(), queries[0]);
        assertEquals("SELECT COUNT(*) AS CNT FROM tx_oracle_ops", queries[1]);
    }

    @Test
    void commentsWithSemicolonsAreIgnoredForSplitting() {
        String sql = """
            SELECT 1; -- this comment has a fake ; here ;
            /* block comment with ; inside */
            SELECT 2;
            """;

        String[] queries = SqlSplitter.getQueries(sql);

        assertEquals(2, queries.length);
        assertEquals("SELECT 1", queries[0]);

        String expectedSecond = """
            -- this comment has a fake ; here ;
            /* block comment with ; inside */
            SELECT 2
            """.trim();

        assertEquals(expectedSecond, queries[1]);
    }

    @Test
    void stringLiteralsWithSemicolons() {
        String sql = "INSERT INTO t(msg) VALUES('hello;world'); SELECT 1;";
        String[] queries = SqlSplitter.getQueries(sql);

        assertEquals(2, queries.length);
        assertEquals("INSERT INTO t(msg) VALUES('hello;world')", queries[0]);
        assertEquals("SELECT 1", queries[1]);
    }

    @Test
    void lastStatementWithoutTrailingSemicolon() {
        String sql = "SELECT 1; SELECT 2";
        String[] queries = SqlSplitter.getQueries(sql);

        assertEquals(2, queries.length);
        assertEquals("SELECT 1", queries[0]);
        assertEquals("SELECT 2", queries[1]);
    }

    @Test
    void postgresJsonDollarQuoteContainingSemicolonMustNotSplit() {
        String sql = """
            SELECT jsonb_array_elements_text($${"data":["ITEM; WITH SEMICOLON"]}$$::jsonb -> 'data') AS value;
            """;

        String[] queries = SqlSplitter.getQueries(sql);

        assertEquals(1, queries.length);
        assertEquals(
            "SELECT jsonb_array_elements_text($${\"data\":[\"ITEM; WITH SEMICOLON\"]}$$::jsonb -> 'data') AS value",
            queries[0]
        );
    }

    @Test
    void postgresTaggedDollarQuoteContainingSemicolonMustNotSplit() {
        String sql = """
            SELECT $json${"data":["ITEM; WITH SEMICOLON"]}$json$::json -> 'data' AS value;
            """;

        String[] queries = SqlSplitter.getQueries(sql);

        assertEquals(1, queries.length);
        assertEquals(
            "SELECT $json${\"data\":[\"ITEM; WITH SEMICOLON\"]}$json$::json -> 'data' AS value",
            queries[0]
        );
    }

    @Test
    void backtickQuotedIdentifiersWithSemicolonsShouldNotSplit() {
        String sql = "SELECT `id;hello` FROM t; SELECT 2;";
        String[] queries = SqlSplitter.getQueries(sql);

        assertEquals(2, queries.length);
        assertEquals("SELECT `id;hello` FROM t", queries[0]);
        assertEquals("SELECT 2", queries[1]);
    }

    @Test
    void sqlServerBracketQuotedIdentifiersWithSemicolonsShouldNotSplit() {
        String sql = "SELECT [id;hello] FROM t; SELECT 2;";
        String[] queries = SqlSplitter.getQueries(sql);

        assertEquals(2, queries.length);
        assertEquals("SELECT [id;hello] FROM t", queries[0]);
        assertEquals("SELECT 2", queries[1]);
    }

    @Test
    void postgresDoubleQuotedIdentifierWithSemicolonMustNotSplit() {
        String sql = """
            -- 1) Setup schema + table
            CREATE SCHEMA IF NOT EXISTS public;

            CREATE TABLE IF NOT EXISTS public.events_demo (
              id        BIGINT PRIMARY KEY,
              event_name TEXT NOT NULL,
              user_id    BIGINT NOT NULL,
              ts_local   TIMESTAMP WITHOUT TIME ZONE,
              ts_zoned   TIMESTAMPTZ
            );

            -- 2) Use quoted identifiers (double quotes)
            ALTER TABLE public.events_demo
              ADD COLUMN IF NOT EXISTS "note;with:semicolon" TEXT;
            """;

        String[] queries = SqlSplitter.getQueries(sql);

        assertEquals(3, queries.length);

        assertEquals("""
            -- 1) Setup schema + table
            CREATE SCHEMA IF NOT EXISTS public
            """.trim(), queries[0]);

        assertEquals("""
            CREATE TABLE IF NOT EXISTS public.events_demo (
              id        BIGINT PRIMARY KEY,
              event_name TEXT NOT NULL,
              user_id    BIGINT NOT NULL,
              ts_local   TIMESTAMP WITHOUT TIME ZONE,
              ts_zoned   TIMESTAMPTZ
            )
            """.trim(), queries[1]);

        assertEquals("""
            -- 2) Use quoted identifiers (double quotes)
            ALTER TABLE public.events_demo
              ADD COLUMN IF NOT EXISTS "note;with:semicolon" TEXT
            """.trim(), queries[2]);
    }
}
