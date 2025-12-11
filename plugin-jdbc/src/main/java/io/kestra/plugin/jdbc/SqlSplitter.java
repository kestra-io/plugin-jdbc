package io.kestra.plugin.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class SqlSplitter {

    public static String[] getQueries(String sql) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();

        int len = sql.length();
        boolean inString = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;
        int beginDepth = 0;

        for (int i = 0; i < len; i++) {
            char c = sql.charAt(i);
            char next = (i + 1 < len) ? sql.charAt(i + 1) : '\0';

            // Handle comments
            if (!inString && !inBlockComment && c == '-' && next == '-') {
                inLineComment = true;
            }
            if (inLineComment && c == '\n') {
                inLineComment = false;
            }
            if (!inString && !inLineComment && c == '/' && next == '*') {
                inBlockComment = true;
            }
            if (inBlockComment && c == '*' && next == '/') {
                inBlockComment = false;
                current.append("*/");
                i++;
                continue;
            }

            if (inLineComment || inBlockComment) {
                current.append(c);
                continue;
            }

            // Handle quoted strings
            if (c == '\'' && !inString) {
                inString = true;
            } else if (c == '\'') {
                inString = false;
            }

            current.append(c);

            // Track PL/SQL BEGIN ... END
            if (!inString) {
                String tail = current.toString().toUpperCase(Locale.ROOT);

                if (tail.endsWith("BEGIN")) {
                    beginDepth++;
                }

                // Detect END;
                if (tail.endsWith("END;")) {
                    beginDepth = Math.max(0, beginDepth - 1);

                    // END of PL/SQL block
                    if (beginDepth == 0) {
                        statements.add(current.toString().trim());
                        current.setLength(0);
                        continue;
                    }
                }
            }

            // Normal SQL statement terminated by ;
            if (beginDepth == 0 && c == ';' && !inString) {
                String s = current.toString().trim();
                if (!s.isEmpty()) {
                    statements.add(s.substring(0, s.length() - 1)); // strip ;
                }
                current.setLength(0);
            }
        }

        // Last statement (no trailing ;)
        String leftover = current.toString().trim();
        if (!leftover.isEmpty()) {
            statements.add(leftover);
        }

        return statements.toArray(new String[0]);
    }
}
