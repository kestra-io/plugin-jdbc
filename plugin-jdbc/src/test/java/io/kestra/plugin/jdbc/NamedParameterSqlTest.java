package io.kestra.plugin.jdbc;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NamedParameterSqlTest {

    @Test
    void replacesDeclaredPlaceholders() {
        var result = NamedParameterSql.parse(
            "SELECT * FROM t WHERE id = :id AND name = :name",
            false, false
        );

        assertEquals("SELECT * FROM t WHERE id = ? AND name = ?", result.sql());
        assertEquals(List.of("id", "name"), result.names());
    }

    @Test
    void samePlaceholderTwiceIsBoundTwice() {
        var result = NamedParameterSql.parse(
            "SELECT :x AS a, :x AS b",
            false, false
        );

        assertEquals("SELECT ? AS a, ? AS b", result.sql());
        assertEquals(List.of("x", "x"), result.names());
    }

    @Test
    void castIsNotAPlaceholder() {
        var result = NamedParameterSql.parse(
            "SELECT '7'::INTEGER AS a, :x AS v",
            false, false
        );

        assertEquals("SELECT '7'::INTEGER AS a, ? AS v", result.sql());
        assertEquals(List.of("x"), result.names());
    }

    @Test
    void castOfDeclaredNameIsNotAPlaceholder() {
        var result = NamedParameterSql.parse(
            "SELECT :x::x FROM t",
            false, false
        );

        assertEquals("SELECT ?::x FROM t", result.sql());
        assertEquals(List.of("x"), result.names());
    }

    @Test
    void colonInsideStringLiteralIsKept() {
        var result = NamedParameterSql.parse(
            "SELECT '10:30' AS t, 'at :x' AS s, :x AS v",
            false, false
        );

        assertEquals("SELECT '10:30' AS t, 'at :x' AS s, ? AS v", result.sql());
        assertEquals(List.of("x"), result.names());
    }

    @Test
    void doubledQuoteInsideLiteralDoesNotEndIt() {
        var result = NamedParameterSql.parse(
            "SELECT 'it''s :x' AS s, :x AS v",
            false, false
        );

        assertEquals("SELECT 'it''s :x' AS s, ? AS v", result.sql());
        assertEquals(List.of("x"), result.names());
    }

    @Test
    void quotedIdentifiersAreKept() {
        var result = NamedParameterSql.parse(
            "SELECT \"a:x\", `b:x` FROM t WHERE c = :x",
            false, false
        );

        assertEquals("SELECT \"a:x\", `b:x` FROM t WHERE c = ?", result.sql());
        assertEquals(List.of("x"), result.names());
    }

    @Test
    void commentsAreKept() {
        var result = NamedParameterSql.parse(
            """
            -- uses :x here
            SELECT /* and :x there */ :x AS v
            """,
            false, false
        );

        assertEquals(
            """
            -- uses :x here
            SELECT /* and :x there */ ? AS v
            """,
            result.sql()
        );
        assertEquals(List.of("x"), result.names());
    }

    @Test
    void everyPlaceholderIsReplacedWhateverIsDeclared() {
        // a parameter rendered to null is dropped from the rendered map, so replacement cannot
        // depend on the declared names
        var result = NamedParameterSql.parse(
            "SELECT :x AS v, :other AS o",
            false, false
        );

        assertEquals("SELECT ? AS v, ? AS o", result.sql());
        assertEquals(List.of("x", "other"), result.names());
    }

    @Test
    void colonGluedToAWordIsNotAPlaceholder() {
        var result = NamedParameterSql.parse(
            "SELECT arr[1:n], payload:n FROM t WHERE k = :n",
            false, false
        );

        assertEquals("SELECT arr[1:n], payload:n FROM t WHERE k = ?", result.sql());
        assertEquals(List.of("n"), result.names());
    }

    @Test
    void placeholderDirectlyAfterOperatorOrParenthesis() {
        var result = NamedParameterSql.parse(
            "INSERT INTO t VALUES(:a,:b) ON CONFLICT DO UPDATE SET v=:b",
            false, false
        );

        assertEquals("INSERT INTO t VALUES(?,?) ON CONFLICT DO UPDATE SET v=?", result.sql());
        assertEquals(List.of("a", "b", "b"), result.names());
    }

    @Test
    void nullOrEmptySqlIsReturnedAsIs() {
        assertNull(NamedParameterSql.parse(null, false, false).sql());
        assertEquals("", NamedParameterSql.parse("", false, false).sql());
        assertTrue(NamedParameterSql.parse("", false, false).names().isEmpty());
    }

    @Test
    void backslashBeforeQuoteIsAnOrdinaryCharacterByDefault() {
        // standard SQL: the backslash does not escape, so the literal ends at the second quote
        var result = NamedParameterSql.parse(
            "SELECT 'C:\\' AS p, :x AS v",
            false, false
        );

        assertEquals("SELECT 'C:\\' AS p, ? AS v", result.sql());
        assertEquals(List.of("x"), result.names());
    }

    @Test
    void backslashEscapedQuoteDoesNotEndLiteralWhenEnabled() {
        // MySQL, MariaDB and ClickHouse: 'it\'s' is one literal
        var result = NamedParameterSql.parse(
            "SELECT 'it\\'s :x' AS s, :x AS v",
            true, false
        );

        assertEquals("SELECT 'it\\'s :x' AS s, ? AS v", result.sql());
        assertEquals(List.of("x"), result.names());
    }

    @Test
    void backslashEscapedQuoteEndsLiteralWhenDisabled() {
        var result = NamedParameterSql.parse(
            "SELECT 'it\\'s :x' AS s, :x AS v",
            false, false
        );

        // the literal closes at the escaped quote and the quote after :x opens an unterminated one
        assertEquals("SELECT 'it\\'s ?' AS s, :x AS v", result.sql());
        assertEquals(List.of("x"), result.names());
    }

    @Test
    void escapeStringPrefixEnablesBackslashEscapes() {
        var result = NamedParameterSql.parse(
            "SELECT E'it\\'s :x' AS a, e'\\\\' AS b, :y AS c",
            false, false
        );

        assertEquals("SELECT E'it\\'s :x' AS a, e'\\\\' AS b, ? AS c", result.sql());
        assertEquals(List.of("y"), result.names());
    }

    @Test
    void letterBeforeQuoteIsNotAPrefixWhenPartOfAWord() {
        // "ONE'" ends in E but is a word, so the literal is a standard one
        var result = NamedParameterSql.parse(
            "SELECT ONE'it\\' AS a, :y AS b",
            false, false
        );

        assertEquals("SELECT ONE'it\\' AS a, ? AS b", result.sql());
        assertEquals(List.of("y"), result.names());
    }

    @Test
    void dollarQuotedBodyIsKept() {
        var result = NamedParameterSql.parse(
            "SELECT $$ it's :x $$ AS a, $tag$ :x $ $tag$ AS b, :y AS c",
            false, false
        );

        assertEquals("SELECT $$ it's :x $$ AS a, $tag$ :x $ $tag$ AS b, ? AS c", result.sql());
        assertEquals(List.of("y"), result.names());
    }

    @Test
    void dollarWithoutClosingTagIsAnOrdinaryCharacter() {
        var result = NamedParameterSql.parse(
            "SELECT $1 AS a, $x AS b, $$ :y",
            false, false
        );

        assertEquals("SELECT $1 AS a, $x AS b, $$ ?", result.sql());
        assertEquals(List.of("y"), result.names());
    }

    @Test
    void hashLineCommentIsKeptWhenEnabled() {
        var result = NamedParameterSql.parse(
            "SELECT :y AS y # :c\nFROM t WHERE k = :y",
            true, true
        );

        assertEquals("SELECT ? AS y # :c\nFROM t WHERE k = ?", result.sql());
        assertEquals(List.of("y", "y"), result.names());
    }

    @Test
    void hashIsAnOrdinaryCharacterByDefault() {
        var result = NamedParameterSql.parse(
            "SELECT :y AS y # :c",
            false, false
        );

        assertEquals("SELECT ? AS y # ?", result.sql());
        assertEquals(List.of("y", "c"), result.names());
    }

    @Test
    void unterminatedLiteralRunsToTheEnd() {
        var result = NamedParameterSql.parse(
            "SELECT 'oops :x",
            false, false
        );

        assertEquals("SELECT 'oops :x", result.sql());
        assertTrue(result.names().isEmpty());
    }
}
