package io.kestra.plugin.jdbc;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

class AbstractJdbcBaseQueryNamedParameterTest {
    @Test
    void replacesNamedParametersInOccurrenceOrder() {
        var parsed = AbstractJdbcBaseQuery.parseNamedParameters(
            "SELECT (:first), :second, :first::INTEGER, :_under_score, :123, :::third"
        );

        assertThat(parsed.sql(), is("SELECT (?), ?, ?::INTEGER, ?, ?, ::?"));
        assertThat(parsed.parameterNames(), contains("first", "second", "first", "_under_score", "123", "third"));
    }

    @Test
    void ignoresParameterLikeTextOutsideOrdinarySql() {
        String sql = """
            SELECT ':string', 'it''s :escaped',
                   "double:identifier", "double""quoted:identifier",
                   `backtick:identifier`, `backtick``quoted:identifier`,
                   [bracket:identifier], [bracket]]quoted:identifier],
                   $$dollar :ignored$$, $tag$tagged :ignored$tag$,
                   value::INTEGER, :actual
            -- :line_comment
            /* :block_comment /* :nested_comment */ :still_block_comment */
            """;

        String expected = """
            SELECT ':string', 'it''s :escaped',
                   "double:identifier", "double""quoted:identifier",
                   `backtick:identifier`, `backtick``quoted:identifier`,
                   [bracket:identifier], [bracket]]quoted:identifier],
                   $$dollar :ignored$$, $tag$tagged :ignored$tag$,
                   value::INTEGER, ?
            -- :line_comment
            /* :block_comment /* :nested_comment */ :still_block_comment */
            """;

        var parsed = AbstractJdbcBaseQuery.parseNamedParameters(sql);

        assertThat(parsed.sql(), is(expected));
        assertThat(parsed.parameterNames(), contains("actual"));
    }

    @Test
    void preservesUnterminatedLexicalConstructs() {
        List<String> sqlFragments = List.of(
            "",
            ":",
            "SELECT ':string",
            "SELECT \"identifier:param",
            "SELECT `identifier:param",
            "SELECT [identifier:param",
            "SELECT $tag$identifier:param",
            "SELECT 1 -- :comment",
            "SELECT 1 /* :comment"
        );

        for (String sql : sqlFragments) {
            var parsed = AbstractJdbcBaseQuery.parseNamedParameters(sql);

            assertThat(parsed.sql(), is(sql));
            assertThat(parsed.parameterNames(), empty());
        }
    }
}
