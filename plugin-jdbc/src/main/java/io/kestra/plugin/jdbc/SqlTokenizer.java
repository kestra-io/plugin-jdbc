package io.kestra.plugin.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Single lexical pass over a SQL statement, shared by {@link SqlTableExtractor} and {@link NamedParameterSql}.
 * <p>
 * It is not a parser: it only tells apart words, quoted identifiers, string literals, comments and punctuation,
 * so that consumers never look inside a literal or a comment. Whitespace produces no token. Every token keeps
 * its offsets in the source, so a consumer can rewrite the SQL without losing a single character.
 * {@link SqlSplitter} deliberately keeps its own scanner, because it must also track statement boundaries.
 */
final class SqlTokenizer {
    private SqlTokenizer() {
    }

    enum TokenType {
        /** Keyword or unquoted identifier. */
        WORD,
        /** T-SQL temp table or variable, such as {@code #tmp} or {@code @id}. */
        VARIABLE,
        /** Digits, with any letters and dots glued to them. */
        NUMBER,
        /** Identifier in double quotes, backticks or, when enabled, brackets. */
        QUOTED,
        /** Single-quoted literal. */
        STRING,
        /** Double-quoted literal, when the dialect reads double quotes as strings (MySQL). */
        DOUBLE_QUOTED_STRING,
        /** Terminated dollar-quoted body, such as {@code $$..$$} or {@code $tag$..$tag$}. */
        DOLLAR_QUOTED,
        /** Line comment or block comment. */
        COMMENT,
        /** Any other single character. */
        SYMBOL
    }

    /**
     * @param type  what the token is
     * @param text  the unquoted value for quoted identifiers and string literals, the raw source otherwise
     * @param start offset of the first character in the source
     * @param end   offset just past the last character in the source
     */
    record Token(TokenType type, String text, int start, int end) {
        boolean isWord(String word) {
            return type == TokenType.WORD && text.equalsIgnoreCase(word);
        }

        boolean isSymbol(String symbol) {
            return type == TokenType.SYMBOL && text.equals(symbol);
        }

        String lower() {
            return text.toLowerCase(Locale.ROOT);
        }
    }

    /**
     * @param backslashEscapes     whether a backslash escapes the next character inside single and double quotes
     *                             (MySQL, MariaDB, ClickHouse); PostgreSQL {@code E'..'} strings always do
     * @param hashLineComments     whether {@code #} starts a line comment (MySQL, MariaDB, ClickHouse)
     * @param doubleQuoteIsString  whether double quotes delimit a string instead of an identifier (MySQL)
     * @param bracketIdentifiers   whether {@code [name]} is a quoted identifier (SQL Server)
     */
    record Options(boolean backslashEscapes, boolean hashLineComments, boolean doubleQuoteIsString, boolean bracketIdentifiers) {
    }

    static List<Token> tokenize(String sql, Options options) {
        var tokens = new ArrayList<Token>();
        var length = sql.length();
        var i = 0;

        while (i < length) {
            var c = sql.charAt(i);
            var next = i + 1 < length ? sql.charAt(i + 1) : '\0';
            var start = i;

            if (Character.isWhitespace(c)) {
                i++;
            } else if (c == '-' && next == '-') {
                i = skipLine(sql, i);
                tokens.add(new Token(TokenType.COMMENT, sql.substring(start, i), start, i));
            } else if (c == '#' && options.hashLineComments()) {
                i = skipLine(sql, i);
                tokens.add(new Token(TokenType.COMMENT, sql.substring(start, i), start, i));
            } else if (c == '/' && next == '*') {
                var end = sql.indexOf("*/", i + 2);
                i = end < 0 ? length : end + 2;
                tokens.add(new Token(TokenType.COMMENT, sql.substring(start, i), start, i));
            } else if (c == '$' && closingDollarQuote(sql, i) > 0) {
                i = closingDollarQuote(sql, i);
                tokens.add(new Token(TokenType.DOLLAR_QUOTED, sql.substring(start, i), start, i));
            } else if (c == '\'') {
                i = scanQuoted(sql, i, '\'', options.backslashEscapes() || isEscapeStringPrefix(sql, i));
                tokens.add(new Token(TokenType.STRING, unquote(sql, start, i, '\''), start, i));
            } else if (c == '"') {
                i = scanQuoted(sql, i, '"', options.backslashEscapes());
                var type = options.doubleQuoteIsString() ? TokenType.DOUBLE_QUOTED_STRING : TokenType.QUOTED;
                tokens.add(new Token(type, unquote(sql, start, i, '"'), start, i));
            } else if (c == '`') {
                // Backticks never take backslash escapes, a doubled backtick is the only escape.
                i = scanQuoted(sql, i, '`', false);
                tokens.add(new Token(TokenType.QUOTED, unquote(sql, start, i, '`'), start, i));
            } else if (c == '[' && options.bracketIdentifiers()) {
                var end = sql.indexOf(']', i + 1);
                i = end < 0 ? length : end + 1;
                tokens.add(new Token(TokenType.QUOTED, sql.substring(start + 1, Math.max(start + 1, i - 1)), start, i));
            } else if (Character.isLetter(c) || c == '_' || c == '#' || c == '@') {
                i++;
                while (i < length && isWordChar(sql.charAt(i))) {
                    i++;
                }
                tokens.add(new Token(c == '#' || c == '@' ? TokenType.VARIABLE : TokenType.WORD, sql.substring(start, i), start, i));
            } else if (Character.isDigit(c)) {
                i++;
                while (i < length && (Character.isLetterOrDigit(sql.charAt(i)) || sql.charAt(i) == '.')) {
                    i++;
                }
                tokens.add(new Token(TokenType.NUMBER, sql.substring(start, i), start, i));
            } else {
                i++;
                tokens.add(new Token(TokenType.SYMBOL, String.valueOf(c), start, i));
            }
        }
        return tokens;
    }

    static boolean isIdentifierStart(char c) {
        return Character.isLetter(c) || c == '_';
    }

    static boolean isIdentifierPart(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    private static boolean isWordChar(char c) {
        return isIdentifierPart(c) || c == '$' || c == '#';
    }

    /**
     * Index just past the closing tag of the dollar-quoted body opening at {@code start}, or -1 when the dollar
     * does not open one ({@code $1}, a dollar glued to a word) or the body never closes. An unterminated body
     * is not a quote: the dollar is then plain punctuation.
     */
    private static int closingDollarQuote(String sql, int start) {
        if (start > 0 && isWordChar(sql.charAt(start - 1))) {
            return -1;
        }
        var tagEnd = start + 1;
        while (tagEnd < sql.length() && isIdentifierPart(sql.charAt(tagEnd))) {
            tagEnd++;
        }
        if (tagEnd >= sql.length() || sql.charAt(tagEnd) != '$' || (tagEnd > start + 1 && Character.isDigit(sql.charAt(start + 1)))) {
            return -1;
        }
        var tag = sql.substring(start, tagEnd + 1);
        var close = sql.indexOf(tag, tagEnd + 1);
        return close < 0 ? -1 : close + tag.length();
    }

    /** Whether the quote at {@code quoteIndex} opens a PostgreSQL escape string ({@code E'..'}). */
    private static boolean isEscapeStringPrefix(String sql, int quoteIndex) {
        if (quoteIndex == 0) {
            return false;
        }
        var prefix = sql.charAt(quoteIndex - 1);
        return (prefix == 'E' || prefix == 'e') && (quoteIndex == 1 || !isIdentifierPart(sql.charAt(quoteIndex - 2)));
    }

    private static int skipLine(String sql, int start) {
        var end = sql.indexOf('\n', start);
        return end < 0 ? sql.length() : end + 1;
    }

    // Returns the index right after the closing quote; doubled quotes are escapes. An unterminated quote runs to the end.
    private static int scanQuoted(String sql, int start, char quote, boolean backslashEscapes) {
        var i = start + 1;
        while (i < sql.length()) {
            var c = sql.charAt(i);
            if (backslashEscapes && c == '\\') {
                i += 2;
                continue;
            }
            if (c == quote) {
                if (i + 1 < sql.length() && sql.charAt(i + 1) == quote) {
                    i += 2;
                    continue;
                }
                return i + 1;
            }
            i++;
        }
        return sql.length();
    }

    private static String unquote(String sql, int start, int end, char quote) {
        var closed = end - 1 > start && sql.charAt(end - 1) == quote;
        var inner = sql.substring(start + 1, closed ? end - 1 : end);
        return inner.replace("" + quote + quote, String.valueOf(quote));
    }
}
