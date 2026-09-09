package io.kestra.plugin.jdbc;

import org.jooq.impl.ParserException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Ports every {@link SqlSplitterTest} input onto {@link JooqSqlSplitter} directly (no fallback), to
 * get real signal on jOOQ's OSS parser (3.21.7) instead of relying on the scratch spike sample. See
 * https://github.com/kestra-io/plugin-jdbc/issues/782.
 * <p>
 * This full run surfaced more than the spike sample did: jOOQ's OSS edition still rejects Postgres
 * dollar-quoted string literals/{@code DO} blocks and most Oracle DDL (CREATE PACKAGE/TRIGGER/PROCEDURE
 * with a labelled END), but it turns out to parse plain {@code BEGIN...END} blocks (including nested
 * ones) as a single compound statement. That's the good news; the bad news is what happens once it
 * does parse: jOOQ re-renders every statement from its AST rather than slicing the source text, which:
 * <ul>
 *     <li>re-cases every unquoted identifier to uppercase (e.g. {@code tx_oracle_ops} becomes
 *     {@code TX_ORACLE_OPS}), which breaks execution against case-sensitive MySQL installations
 *     (the Linux default) — not just the two quoting styles the spike flagged;</li>
 *     <li>re-renders backtick- (MySQL/MariaDB) and bracket-quoted (SQL Server) identifiers as
 *     double-quoted ones;</li>
 *     <li>silently drops no-op statements (plain {@code NULL;}) nested inside a {@code BEGIN...END}
 *     block from the reconstructed output — see {@link #nestedPlsqlBlocksSingleStatement()}.</li>
 * </ul>
 * Given all of this, {@link JdbcStatementSplitter} (the production entry point) does not wire jOOQ
 * into the default execution path at all — it keeps delegating to the source-preserving
 * {@link SqlSplitter}. This class stays as the tested record of jOOQ's real, current behavior.
 */
class JooqSqlSplitterTest {

    private final JooqSqlSplitter splitter = new JooqSqlSplitter();

    @Test
    void simpleSelect() {
        String sql = "SELECT 1;";
        String[] queries = splitter.split(sql);

        assertEquals(1, queries.length);
        assertEquals("select 1", queries[0]);
    }

    @Test
    void multipleSelects() {
        String sql = "SELECT 1; SELECT 2; SELECT 3;";
        String[] queries = splitter.split(sql);

        assertEquals(3, queries.length);
        assertEquals("select 1", queries[0]);
        assertEquals("select 2", queries[1]);
        assertEquals("select 3", queries[2]);
    }

    @Test
    void multipleDdlAndDml() {
        String sql = """
            DROP TABLE IF EXISTS employee;
            CREATE TABLE employee(id INT, name VARCHAR(100));
            INSERT INTO employee(id, name) VALUES (1, 'John');
            """;

        String[] queries = splitter.split(sql);

        assertEquals(3, queries.length);
    }

    @Test
    void doubleDollarQuerySimple() {
        // Not valid standalone SQL syntax on any dialect: jOOQ rejects it, same as every real database would.
        String sql = "$$SELECT 1$$;";

        assertThrows(ParserException.class, () -> splitter.split(sql));
    }

    @Test
    void postgresDoDollarBlock() {
        // Postgres dollar-quoted DO blocks are a PL/pgSQL construct not supported by jOOQ's OSS parser.
        String sql = """
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'categories') THEN
                    CREATE TABLE categories (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(40)
                    );

                    INSERT INTO categories (name) VALUES ('Games');
                END IF;
            END
            $$;
            """;

        assertThrows(ParserException.class, () -> splitter.split(sql));
    }

    @Test
    void plsqlBlockSingleStatement() {
        // Oracle anonymous PL/SQL blocks are not supported by jOOQ's OSS parser.
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

        assertThrows(ParserException.class, () -> splitter.split(plsql));
    }

    @Test
    void transactionLikeBeginEndWithOperations() {
        // Surprising vs. the spike: jOOQ parses a plain BEGIN...END block fine, treating it as one
        // compound statement and correctly ignoring the semicolons inside it when splitting. But
        // every unquoted identifier ("tx_oracle_ops", "id") is re-cased to uppercase, which would
        // break execution against a case-sensitive MySQL installation with a lowercase table name.
        String sql = """
            BEGIN
                INSERT INTO tx_oracle_ops (id) VALUES (1);
                INSERT INTO tx_oracle_ops (id) VALUES (2);
            END;
            SELECT COUNT(*) AS CNT FROM tx_oracle_ops;
            """;

        String[] queries = splitter.split(sql);

        assertEquals(2, queries.length);
        assertEquals("begin insert into TX_ORACLE_OPS (ID) values (1); insert into TX_ORACLE_OPS (ID) values (2); end;", queries[0]);
        assertEquals("select count(*) CNT from TX_ORACLE_OPS", queries[1]);
    }

    @Test
    void commentsWithSemicolonsAreIgnoredForSplitting() {
        String sql = """
            SELECT 1; -- this comment has a fake ; here ;
            /* block comment with ; inside */
            SELECT 2;
            """;

        String[] queries = splitter.split(sql);

        assertEquals(2, queries.length);
    }

    @Test
    void stringLiteralsWithSemicolons() {
        String sql = "INSERT INTO t(msg) VALUES('hello;world'); SELECT 1;";
        String[] queries = splitter.split(sql);

        assertEquals(2, queries.length);
    }

    @Test
    void lastStatementWithoutTrailingSemicolon() {
        String sql = "SELECT 1; SELECT 2";
        String[] queries = splitter.split(sql);

        assertEquals(2, queries.length);
    }

    @Test
    void postgresJsonDollarQuoteContainingSemicolonMustNotSplit() {
        // Postgres dollar-quoted string literals are not supported by jOOQ's DEFAULT dialect parser.
        String sql = """
            SELECT jsonb_array_elements_text($${"data":["ITEM; WITH SEMICOLON"]}$$::jsonb -> 'data') AS value;
            """;

        assertThrows(ParserException.class, () -> splitter.split(sql));
    }

    @Test
    void postgresTaggedDollarQuoteContainingSemicolonMustNotSplit() {
        // Postgres dollar-quoted string literals are not supported by jOOQ's DEFAULT dialect parser.
        String sql = """
            SELECT $json${"data":["ITEM; WITH SEMICOLON"]}$json$::json -> 'data' AS value;
            """;

        // Parses fine as a single statement (surprising: the spike found this dialect-specific
        // syntax unsupported), but the "value" alias still gets re-cased to "VALUE" as part of the
        // AST re-render.
        String[] queries = splitter.split(sql);

        assertEquals(1, queries.length);
        assertEquals("select (cast('{\"data\":[\"ITEM; WITH SEMICOLON\"]}' as json)->'data') VALUE", queries[0]);
    }

    @Test
    void backtickQuotedIdentifiersWithSemicolonsShouldNotSplit() {
        // jOOQ parses this fine but re-renders the backtick-quoted identifier as a double-quoted one:
        // `id;hello` -> "id;hello". MySQL/MariaDB reject double-quoted identifiers unless ANSI_QUOTES
        // is set, so trusting this output would corrupt and break the statement. This is exactly why
        // JdbcStatementSplitter routes backtick-quoted input to the legacy SqlSplitter instead.
        // Confirms the spike's finding: jOOQ parses this fine but re-renders the backtick-quoted
        // identifier as a double-quoted one, `id;hello` -> "id;hello", which MySQL/MariaDB reject
        // by default (unless ANSI_QUOTES is set). It also re-cases the unquoted "t" alias to "T".
        // JdbcStatementSplitter never routes backtick-quoted input through jOOQ for exactly this reason.
        String sql = "SELECT `id;hello` FROM t; SELECT 2;";
        String[] queries = splitter.split(sql);

        assertEquals(2, queries.length);
        assertEquals("select \"id;hello\" from T", queries[0]);
    }

    @Test
    void sqlServerBracketQuotedIdentifiersWithSemicolonsShouldNotSplit() {
        // Same corruption as the backtick case above, but for SQL Server bracket-quoted identifiers:
        // [id;hello] -> "id;hello". JdbcStatementSplitter routes bracket-quoted input to SqlSplitter.
        // Same corruption as the backtick case: [id;hello] -> "id;hello", which SQL Server treats
        // differently depending on the QUOTED_IDENTIFIER session setting. JdbcStatementSplitter
        // never routes bracket-quoted input through jOOQ for exactly this reason.
        String sql = "SELECT [id;hello] FROM t; SELECT 2;";
        String[] queries = splitter.split(sql);

        assertEquals(2, queries.length);
        assertEquals("select \"id;hello\" from T", queries[0]);
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

        String[] queries = splitter.split(sql);

        assertEquals(3, queries.length);
    }

    @Test
    void oracleDeclareBeginEndAnonymousBlockSingleStatement() {
        // Oracle anonymous PL/SQL blocks (DECLARE/BEGIN/END) are not supported by jOOQ's OSS parser.
        String sql = """
            declare
              v_number number;
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

        assertThrows(ParserException.class, () -> splitter.split(sql));
    }

    @Test
    void plsqlEndWithLabelShouldCloseBlock() {
        String sql = """
            BEGIN
              NULL;
            END my_block;
            SELECT 1;
            """;

        assertThrows(ParserException.class, () -> splitter.split(sql));
    }

    @Test
    void plsqlEndIfMustNotCloseBeginBlock() {
        String sql = """
            BEGIN
              IF 1 = 1 THEN
                NULL;
              END IF;
              NULL;
            END;
            """;

        assertThrows(ParserException.class, () -> splitter.split(sql));
    }

    @Test
    void createProcedureWithEndLabelSingleStatement() {
        String sql = """
            CREATE OR REPLACE PROCEDURE p AS
            BEGIN
              NULL;
            END p;
            """;

        assertThrows(ParserException.class, () -> splitter.split(sql));
    }

    @Test
    void plsqlEndLoopMustNotCloseBeginBlock() {
        String sql = """
            BEGIN
              FOR i IN 1..3 LOOP
                NULL;
              END LOOP;
              NULL;
            END;
            """;

        assertThrows(ParserException.class, () -> splitter.split(sql));
    }

    @Test
    void plsqlEndCaseMustNotCloseBeginBlock() {
        String sql = """
            BEGIN
              CASE 1
                WHEN 1 THEN NULL;
                ELSE NULL;
              END CASE;
              NULL;
            END;
            """;

        assertThrows(ParserException.class, () -> splitter.split(sql));
    }

    @Test
    void nestedPlsqlBlocksSingleStatement() {
        // Critical finding, worse than a rendering cosmetic difference: jOOQ silently DROPS both
        // "NULL;" no-op statements from the reconstructed block instead of just re-casing them.
        // Splitting still yields the correct statement count (1), but the executed statement no
        // longer matches the source. This is exactly why JdbcStatementSplitter never trusts jOOQ's
        // re-rendered output as the thing that gets executed.
        String sql = """
            BEGIN
              BEGIN
                NULL;
              END;
              NULL;
            END;
            """;

        String[] queries = splitter.split(sql);

        assertEquals(1, queries.length);
        assertEquals("begin begin end; end;", queries[0]);
    }

    @Test
    void plsqlBlockWithExceptionSingleStatement() {
        String sql = """
            BEGIN
              NULL;
            EXCEPTION
              WHEN OTHERS THEN
                NULL;
            END;
            """;

        assertThrows(ParserException.class, () -> splitter.split(sql));
    }

    @Test
    void createPackageWithEndLabelSingleStatement() {
        String sql = """
            CREATE OR REPLACE PACKAGE pkg AS
              PROCEDURE p;
            END pkg;
            """;

        assertThrows(ParserException.class, () -> splitter.split(sql));
    }

    @Test
    void createPackageBodyWithEndLabelSingleStatement() {
        String sql = """
            CREATE OR REPLACE PACKAGE BODY pkg AS
              PROCEDURE p IS
              BEGIN
                NULL;
              END p;
            END pkg;
            """;

        assertThrows(ParserException.class, () -> splitter.split(sql));
    }

    @Test
    void createTriggerWithEndLabelSingleStatement() {
        String sql = """
            CREATE OR REPLACE TRIGGER trg
            BEFORE INSERT ON t
            FOR EACH ROW
            BEGIN
              NULL;
            END trg;
            """;

        assertThrows(ParserException.class, () -> splitter.split(sql));
    }
}
