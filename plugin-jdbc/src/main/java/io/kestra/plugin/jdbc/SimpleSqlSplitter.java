package io.kestra.plugin.jdbc;

import io.kestra.sql.grammar.SQLSplitLexer;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;

import java.util.ArrayList;
import java.util.List;

public class SimpleSqlSplitter {

    public static List<String> split(String script) {
        SQLSplitLexer lexer = new SQLSplitLexer(CharStreams.fromString(script));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();

        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();

        int blockDepth = 0;
        int prevType = -1;

        for (Token token : tokens.getTokens()) {
            int type = token.getType();

            if (type == Token.EOF) {
                break;
            }

            String text = token.getText();

            // PostgreSQL dollar-quoted block: treat as opaque
            if (type == SQLSplitLexer.DOLLAR_QUOTE) {
                current.append(text);
                prevType = type;
                continue; // no delimiters inside
            }

            // Track procedural blocks (Postgres + Oracle / PL/SQL)
            if (type == SQLSplitLexer.BEGIN_KW) {
                blockDepth++;
            } else if (type == SQLSplitLexer.END_KW) {
                blockDepth = Math.max(0, blockDepth - 1);
            }

            boolean safeDelimiter =
                (type == SQLSplitLexer.SEMICOLON)
                    && blockDepth == 0;

            if (safeDelimiter) {
                // Si on vient de voir un END_KW, on garde le ';'
                if (prevType == SQLSplitLexer.END_KW) {
                    current.append(text);
                }

                String statement = current.toString().trim();
                if (!statement.isEmpty()) {
                    result.add(statement);
                }
                current.setLength(0);
            } else {
                current.append(text);
            }

            prevType = type;
        }

        String last = current.toString().trim();
        if (!last.isEmpty()) {
            result.add(last);
        }

        return result;
    }
}
