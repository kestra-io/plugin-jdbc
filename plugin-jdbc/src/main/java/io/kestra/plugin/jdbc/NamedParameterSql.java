package io.kestra.plugin.jdbc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Rewrites {@code :name} placeholders into JDBC {@code ?} markers.
 * <p>
 * Every {@code :name} is replaced when the colon starts a placeholder: string literals
 * ({@code '10:30'}, {@code E'..'}, {@code $tag$..$tag$}), quoted identifiers, line and block
 * comments (including {@code #} line comments when enabled), casts ({@code value::INTEGER}) and
 * colons glued to a preceding word ({@code arr[1:n]}, Snowflake {@code col:field}) are left
 * untouched. Replacement does not depend on the declared parameters, because a parameter whose
 * value renders to null is dropped from the rendered map and still has to be bound.
 *
 * @param sql   the SQL with placeholders replaced by {@code ?}
 * @param names the parameter names in the order their markers appear, one entry per marker
 */
record NamedParameterSql(String sql, List<String> names) {

    /**
     * @param sql              the SQL to rewrite
     * @param backslashEscapes whether a backslash escapes the next character inside a quoted
     *                         literal, as MySQL, MariaDB and ClickHouse do ({@code 'it\'s'})
     * @param hashLineComments whether {@code #} starts a line comment, as in MySQL, MariaDB and
     *                         ClickHouse
     */
    static NamedParameterSql parse(String sql, boolean backslashEscapes, boolean hashLineComments) {
        if (sql == null || sql.isEmpty()) {
            return new NamedParameterSql(sql, Collections.emptyList());
        }

        var len = sql.length();
        var out = new StringBuilder(len);
        var names = new ArrayList<String>();

        var i = 0;
        while (i < len) {
            var c = sql.charAt(i);
            var next = i + 1 < len ? sql.charAt(i + 1) : '\0';

            if (c == '\'' || c == '"' || c == '`') {
                var end = closingQuote(sql, i, c, backslashEscapes || isEscapeStringPrefix(sql, i));
                out.append(sql, i, end);
                i = end;
            } else if (c == '$') {
                var end = closingDollarQuote(sql, i);
                out.append(sql, i, Math.max(end, i + 1));
                i = Math.max(end, i + 1);
            } else if ((c == '-' && next == '-') || (c == '#' && hashLineComments)) {
                var end = sql.indexOf('\n', i);
                end = end < 0 ? len : end;
                out.append(sql, i, end);
                i = end;
            } else if (c == '/' && next == '*') {
                var end = sql.indexOf("*/", i + 2);
                end = end < 0 ? len : end + 2;
                out.append(sql, i, end);
                i = end;
            } else if (c == ':' && next == ':') {
                out.append("::");
                i += 2;
            } else if (c == ':' && isIdentifierStart(next) && (i == 0 || !isIdentifierPart(sql.charAt(i - 1)))) {
                var end = i + 1;
                while (end < len && isIdentifierPart(sql.charAt(end))) {
                    end++;
                }
                out.append('?');
                names.add(sql.substring(i + 1, end));
                i = end;
            } else {
                out.append(c);
                i++;
            }
        }

        return new NamedParameterSql(out.toString(), Collections.unmodifiableList(names));
    }

    /**
     * Index just past the closing quote, treating a doubled quote ({@code ''}) and, when enabled,
     * a backslash-escaped character ({@code \'}) as part of the literal. An unterminated quote
     * runs to the end of the SQL.
     */
    private static int closingQuote(String sql, int start, char quote, boolean backslashEscapes) {
        var len = sql.length();
        var i = start + 1;
        while (i < len) {
            var c = sql.charAt(i);
            if (backslashEscapes && c == '\\') {
                i += 2;
                continue;
            }
            if (c == quote) {
                if (i + 1 < len && sql.charAt(i + 1) == quote) {
                    i += 2;
                    continue;
                }
                return i + 1;
            }
            i++;
        }
        return len;
    }

    /**
     * Whether the quote at {@code quoteIndex} opens a PostgreSQL escape string ({@code E'..'}),
     * in which a backslash escapes the next character.
     */
    private static boolean isEscapeStringPrefix(String sql, int quoteIndex) {
        if (quoteIndex == 0 || sql.charAt(quoteIndex) != '\'') {
            return false;
        }
        var prefix = sql.charAt(quoteIndex - 1);
        return (prefix == 'E' || prefix == 'e') && (quoteIndex == 1 || !isIdentifierPart(sql.charAt(quoteIndex - 2)));
    }

    /**
     * Index just past the closing tag of a dollar-quoted body ({@code $$..$$}, {@code $tag$..$tag$})
     * opening at {@code start}, or -1 when the dollar does not open one or the body never closes.
     */
    private static int closingDollarQuote(String sql, int start) {
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

    private static boolean isIdentifierStart(char c) {
        return Character.isLetter(c) || c == '_';
    }

    private static boolean isIdentifierPart(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }
}
