package io.kestra.plugin.jdbc.explorer;

public final class SqlText {
    private SqlText() {
    }

    /**
     * Blanks string literals, quoted identifiers and comments so that keyword checks only see SQL code.
     */
    public static String stripLiteralsAndComments(String sql) {
        var out = new StringBuilder(sql.length());
        var i = 0;
        while (i < sql.length()) {
            var c = sql.charAt(i);
            if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
                var end = sql.indexOf('\n', i);
                i = end < 0 ? sql.length() : end;
                out.append(' ');
            } else if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
                var end = sql.indexOf("*/", i + 2);
                i = end < 0 ? sql.length() : end + 2;
                out.append(' ');
            } else if (c == '\'' || c == '"' || c == '`') {
                var j = i + 1;
                while (j < sql.length()) {
                    if (sql.charAt(j) == c) {
                        if (j + 1 < sql.length() && sql.charAt(j + 1) == c) {
                            j += 2;
                            continue;
                        }
                        break;
                    }
                    j++;
                }
                i = Math.min(sql.length(), j + 1);
                out.append(" \"\" ");
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }
}
