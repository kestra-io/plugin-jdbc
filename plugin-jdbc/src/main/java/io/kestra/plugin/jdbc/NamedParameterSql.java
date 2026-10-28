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

        var tokens = SqlTokenizer.tokenize(sql, new SqlTokenizer.Options(backslashEscapes, hashLineComments, false, false));
        var len = sql.length();
        var out = new StringBuilder(len);
        var names = new ArrayList<String>();

        // Literals, comments and quoted identifiers are tokens of their own, so only bare colons are looked at.
        var copied = 0;
        var skipUntil = 0;
        for (var token : tokens) {
            var i = token.start();
            if (token.type() != SqlTokenizer.TokenType.SYMBOL || token.text().charAt(0) != ':' || i < skipUntil || i < copied) {
                continue;
            }
            var next = i + 1 < len ? sql.charAt(i + 1) : '\0';
            if (next == ':') {
                skipUntil = i + 2;
            } else if (SqlTokenizer.isIdentifierStart(next) && (i == 0 || !SqlTokenizer.isIdentifierPart(sql.charAt(i - 1)))) {
                var end = i + 1;
                while (end < len && SqlTokenizer.isIdentifierPart(sql.charAt(end))) {
                    end++;
                }
                out.append(sql, copied, i).append('?');
                names.add(sql.substring(i + 1, end));
                copied = end;
            }
        }
        out.append(sql, copied, len);

        return new NamedParameterSql(out.toString(), Collections.unmodifiableList(names));
    }
}
