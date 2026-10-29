package io.kestra.plugin.jdbc;

import io.kestra.plugin.jdbc.SqlTokenizer.Options;
import io.kestra.plugin.jdbc.SqlTokenizer.Token;
import io.kestra.plugin.jdbc.SqlTokenizer.TokenType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlTokenizerTest {
    private static final Options PLAIN = new Options(false, false, false, false);

    private static List<TokenType> types(String sql, Options options) {
        return SqlTokenizer.tokenize(sql, options).stream().map(Token::type).toList();
    }

    @Test
    void splitsWordsLiteralsCommentsAndPunctuation() {
        var tokens = SqlTokenizer.tokenize("SELECT \"a b\", 'it''s' /* c */ FROM t -- end", PLAIN);

        assertEquals(
            List.of(TokenType.WORD, TokenType.QUOTED, TokenType.SYMBOL, TokenType.STRING, TokenType.COMMENT, TokenType.WORD, TokenType.WORD, TokenType.COMMENT),
            tokens.stream().map(Token::type).toList()
        );
        assertEquals("a b", tokens.get(1).text());
        assertEquals("it's", tokens.get(3).text());
    }

    @Test
    void tokensKeepTheirSourceOffsets() {
        var sql = "SELECT  x";
        var tokens = SqlTokenizer.tokenize(sql, PLAIN);

        assertEquals("x", sql.substring(tokens.get(1).start(), tokens.get(1).end()));
    }

    @Test
    void hashIsACommentOnlyWhenEnabled() {
        assertEquals(List.of(TokenType.WORD, TokenType.COMMENT), types("a # note", new Options(false, true, false, false)));
        assertEquals(List.of(TokenType.WORD, TokenType.VARIABLE, TokenType.WORD), types("a #tmp note", PLAIN));
    }

    @Test
    void doubleQuoteKindFollowsTheDialect() {
        assertEquals(List.of(TokenType.QUOTED), types("\"a\"", PLAIN));
        assertEquals(List.of(TokenType.DOUBLE_QUOTED_STRING), types("\"a\"", new Options(false, false, true, false)));
    }

    @Test
    void bracketsAreIdentifiersOnlyWhenEnabled() {
        assertEquals(List.of(TokenType.QUOTED), types("[a b]", new Options(false, false, false, true)));
        assertEquals(List.of(TokenType.SYMBOL, TokenType.WORD, TokenType.WORD, TokenType.SYMBOL), types("[a b]", PLAIN));
    }

    @Test
    void backslashEscapesApplyToStringsButNotToBackticks() {
        var escaping = new Options(true, false, false, false);

        assertEquals(1, SqlTokenizer.tokenize("'it\\'s'", escaping).size());
        assertEquals(List.of(TokenType.QUOTED, TokenType.WORD), types("`a\\` b", escaping));
    }

    @Test
    void escapeStringPrefixMustNotBeGluedToAWord() {
        var escaped = SqlTokenizer.tokenize("E'it\\'s'", PLAIN);
        assertEquals(List.of(TokenType.WORD, TokenType.STRING), escaped.stream().map(Token::type).toList());
        assertEquals("it\\'s", escaped.get(1).text());
        // ONE is a word, so the quote is a plain one and closes at the escaped quote
        assertEquals(List.of(TokenType.WORD, TokenType.STRING, TokenType.WORD), types("ONE'it\\'s'", PLAIN).subList(0, 3));
    }

    @Test
    void terminatedDollarQuotesAreOneToken() {
        assertEquals(List.of(TokenType.DOLLAR_QUOTED, TokenType.WORD), types("$tag$ ' $tag$ x", PLAIN));
    }

    @Test
    void unterminatedDollarQuoteIsNotAQuote() {
        assertEquals(List.of(TokenType.SYMBOL, TokenType.SYMBOL, TokenType.SYMBOL, TokenType.WORD), types("$$ :y", PLAIN));
    }

    @Test
    void positionalParametersAreNotDollarQuotes() {
        assertEquals(List.of(TokenType.SYMBOL, TokenType.NUMBER), types("$1", PLAIN));
    }
}
