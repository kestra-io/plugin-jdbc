package io.kestra.plugin.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class SqlSplitter {

    public static String[] getQueries(String sql) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();

        int len = sql.length();

        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;

        // Postgres dollar-quoted strings: $$...$$ or $tag$...$tag$
        boolean inDollarQuote = false;
        String dollarTag = null; // e.g. "$$" or "$json$"

        boolean inBacktick = false;
        boolean inSquareBracket = false;

        int beginDepth = 0;
        boolean sawEndToken = false;
        StringBuilder token = new StringBuilder();

        boolean doDollarBlock = false;
        String lastTokenUpper = null;

        // Oracle PL/SQL: DECLARE ... BEGIN ... END; is a single anonymous block.
        // We treat DECLARE as opening a block, and the subsequent BEGIN as the same block (no extra nesting).
        boolean inDeclareSection = false;

        // Oracle PL/SQL DDL (PACKAGE / PACKAGE BODY / PROCEDURE / FUNCTION / TRIGGER / TYPE):
        // Those can contain semicolons without any top-level BEGIN...END. Only the final END ...; ends the statement.
        int plsqlDdlDepth = 0;

        // Track CREATE [OR REPLACE] <object_type>
        boolean sawCreate = false;
        boolean sawOr = false;
        boolean sawReplace = false;

        // Small mutable holder for lastTokenUpper (so helper can update it).
        String[] lastTokenHolder = new String[1];
        lastTokenHolder[0] = null;

        for (int i = 0; i < len; i++) {
            char c = sql.charAt(i);
            char next = (i + 1 < len) ? sql.charAt(i + 1) : '\0';

            // Exit line comment
            if (inLineComment) {
                current.append(c);
                if (c == '\n') {
                    inLineComment = false;
                }
                continue;
            }

            // Exit block comment
            if (inBlockComment) {
                current.append(c);
                if (c == '*' && next == '/') {
                    current.append('/');
                    i++;
                    inBlockComment = false;
                }
                continue;
            }

            // Inside dollar-quote: only look for its closing tag
            if (inDollarQuote) {
                current.append(c);

                // If the remaining substring starts with the closing tag, close it.
                if (c == '$' && dollarTag != null) {
                    int tagLen = dollarTag.length();
                    if (i + tagLen <= len && sql.startsWith(dollarTag, i)) {
                        // we already appended current char '$', append rest of tag (excluding first '$')
                        for (int k = 1; k < tagLen; k++) {
                            current.append(sql.charAt(i + k));
                        }
                        i += tagLen - 1;
                        inDollarQuote = false;
                        dollarTag = null;
                    }
                }
                continue;
            }

            if (inBacktick) {
                current.append(c);

                // MySQL escape for backtick inside identifier is `` (double backtick)
                if (c == '`') {
                    if (next == '`') {
                        current.append(next);
                        i++;
                    } else {
                        inBacktick = false;
                    }
                }
                continue;
            }

            if (inSquareBracket) {
                current.append(c);

                // SQL Server escape for ']' inside identifier is ']]'
                if (c == ']') {
                    if (next == ']') {
                        current.append(next);
                        i++;
                    } else {
                        inSquareBracket = false;
                    }
                }
                continue;
            }

            // Double-quoted identifier (Postgres)
            if (!inSingleQuote && c == '"') {
                TokenState state = flushToken(token, beginDepth, sawEndToken, inDeclareSection,
                    plsqlDdlDepth, sawCreate, sawOr, sawReplace, lastTokenHolder);
                beginDepth = state.beginDepth;
                sawEndToken = state.sawEndToken;
                inDeclareSection = state.inDeclareSection;
                plsqlDdlDepth = state.plsqlDdlDepth;
                sawCreate = state.sawCreate;
                sawOr = state.sawOr;
                sawReplace = state.sawReplace;

                current.append(c);

                if (inDoubleQuote) {
                    // Escaped double quote inside identifier => ""
                    if (next == '"') {
                        current.append(next);
                        i++;
                    } else {
                        inDoubleQuote = false;
                    }
                } else {
                    inDoubleQuote = true;
                }
                continue;
            }

            // Start comments (only when not in a single quote)
            if (!inSingleQuote && !inDoubleQuote && c == '-' && next == '-') {
                TokenState state = flushToken(token, beginDepth, sawEndToken, inDeclareSection,
                    plsqlDdlDepth, sawCreate, sawOr, sawReplace, lastTokenHolder);
                beginDepth = state.beginDepth;
                sawEndToken = state.sawEndToken;
                inDeclareSection = state.inDeclareSection;
                plsqlDdlDepth = state.plsqlDdlDepth;
                sawCreate = state.sawCreate;
                sawOr = state.sawOr;
                sawReplace = state.sawReplace;

                inLineComment = true;
                current.append(c); // keep the comment text
                continue;
            }

            if (!inSingleQuote && !inDoubleQuote && c == '/' && next == '*') {
                TokenState state = flushToken(token, beginDepth, sawEndToken, inDeclareSection,
                    plsqlDdlDepth, sawCreate, sawOr, sawReplace, lastTokenHolder);
                beginDepth = state.beginDepth;
                sawEndToken = state.sawEndToken;
                inDeclareSection = state.inDeclareSection;
                plsqlDdlDepth = state.plsqlDdlDepth;
                sawCreate = state.sawCreate;
                sawOr = state.sawOr;
                sawReplace = state.sawReplace;

                inBlockComment = true;
                current.append(c); // keep the comment text
                continue;
            }

            // Start / end single-quoted strings (handle doubled quotes '')
            if (c == '\'') {
                TokenState state = flushToken(token, beginDepth, sawEndToken, inDeclareSection,
                    plsqlDdlDepth, sawCreate, sawOr, sawReplace, lastTokenHolder);
                beginDepth = state.beginDepth;
                sawEndToken = state.sawEndToken;
                inDeclareSection = state.inDeclareSection;
                plsqlDdlDepth = state.plsqlDdlDepth;
                sawCreate = state.sawCreate;
                sawOr = state.sawOr;
                sawReplace = state.sawReplace;

                current.append(c);

                if (inSingleQuote) {
                    // If next is also a quote => escaped quote inside the string
                    if (next == '\'') {
                        current.append(next);
                        i++;
                    } else {
                        inSingleQuote = false;
                    }
                } else {
                    inSingleQuote = true;
                }
                continue;
            }

            // Backtick-quoted identifier (MySQL/MariaDB/Snowflake/BigQuery)
            if (!inSingleQuote && !inDoubleQuote && c == '`') {
                // flush token
                TokenState state = flushToken(token, beginDepth, sawEndToken, inDeclareSection,
                    plsqlDdlDepth, sawCreate, sawOr, sawReplace, lastTokenHolder);
                beginDepth = state.beginDepth;
                sawEndToken = state.sawEndToken;
                inDeclareSection = state.inDeclareSection;
                plsqlDdlDepth = state.plsqlDdlDepth;
                sawCreate = state.sawCreate;
                sawOr = state.sawOr;
                sawReplace = state.sawReplace;
                lastTokenUpper = state.lastTokenUpper;

                inBacktick = true;
                current.append(c);
                continue;
            }

            // SQL Server bracket-quoted identifier
            if (!inSingleQuote && !inDoubleQuote && c == '[') {
                TokenState state = flushToken(token, beginDepth, sawEndToken, inDeclareSection,
                    plsqlDdlDepth, sawCreate, sawOr, sawReplace, lastTokenHolder);
                beginDepth = state.beginDepth;
                sawEndToken = state.sawEndToken;
                inDeclareSection = state.inDeclareSection;
                plsqlDdlDepth = state.plsqlDdlDepth;
                sawCreate = state.sawCreate;
                sawOr = state.sawOr;
                sawReplace = state.sawReplace;

                inSquareBracket = true;
                current.append(c);
                continue;
            }

            // Start Postgres dollar-quoted string (only when not in a single quote)
            if (!inSingleQuote && !inDoubleQuote && c == '$') {
                // detect $tag$ (including $$)
                int closing = sql.indexOf('$', i + 1);
                if (closing != -1) {
                    String candidate = sql.substring(i, closing + 1); // "$$" or "$tag$"
                    // Valid tag: starts/ends with $, contains only letters/digits/underscore between
                    if (candidate.length() >= 2 && isValidDollarTag(candidate)) {
                        TokenState state = flushToken(token, beginDepth, sawEndToken, inDeclareSection,
                            plsqlDdlDepth, sawCreate, sawOr, sawReplace, lastTokenHolder);
                        beginDepth = state.beginDepth;
                        sawEndToken = state.sawEndToken;
                        inDeclareSection = state.inDeclareSection;
                        plsqlDdlDepth = state.plsqlDdlDepth;
                        sawCreate = state.sawCreate;
                        sawOr = state.sawOr;
                        sawReplace = state.sawReplace;
                        lastTokenUpper = state.lastTokenUpper;

                        if ("DO".equals(lastTokenUpper)) {
                            doDollarBlock = true;
                        }

                        inDollarQuote = true;
                        dollarTag = candidate;
                        current.append(candidate);
                        i = closing; // we consumed up to the second '$'
                        continue;
                    }
                }
            }

            // Normal character
            current.append(c);

            // Track BEGIN/END only outside strings/comments/dollar
            if (!inSingleQuote && !inDoubleQuote) {
                if (isWordChar(c)) {
                    token.append(c);
                } else {
                    if (!token.isEmpty()) {
                        TokenState state = flushToken(token, beginDepth, sawEndToken, inDeclareSection,
                            plsqlDdlDepth, sawCreate, sawOr, sawReplace, lastTokenHolder);
                        beginDepth = state.beginDepth;
                        sawEndToken = state.sawEndToken;
                        inDeclareSection = state.inDeclareSection;
                        plsqlDdlDepth = state.plsqlDdlDepth;
                        sawCreate = state.sawCreate;
                        sawOr = state.sawOr;
                        sawReplace = state.sawReplace;
                    }

                    if (c == ';') {
                        // Close PL/SQL DDL unit on final "END ...;"
                        if (plsqlDdlDepth > 0 && sawEndToken) {
                            plsqlDdlDepth = Math.max(0, plsqlDdlDepth - 1);
                            sawEndToken = false;

                            if (plsqlDdlDepth == 0 && beginDepth == 0) {
                                statements.add(current.toString().trim());
                                current.setLength(0);
                                doDollarBlock = false;
                                inDeclareSection = false; // reset oracle declare state
                                continue;
                            }
                        }

                        if (beginDepth > 0 && sawEndToken) {
                            beginDepth = Math.max(0, beginDepth - 1);
                            sawEndToken = false;

                            if (beginDepth == 0) {
                                statements.add(current.toString().trim());
                                current.setLength(0);
                                doDollarBlock = false;
                                inDeclareSection = false; // reset oracle declare state
                                continue;
                            }
                        } else {
                            sawEndToken = false;
                        }
                    } else if (!Character.isWhitespace(c)) {
                        sawEndToken = false;
                    }
                }
            }

            // Split on semicolon only when not inside string and not inside BEGIN...END
            // (and not inside PL/SQL DDL unit like CREATE PACKAGE / CREATE TRIGGER ...)
            if (beginDepth == 0 && plsqlDdlDepth == 0 && c == ';' && !inSingleQuote && !inDoubleQuote) {
                String s = current.toString().trim();
                if (!s.isEmpty()) {
                    // strip trailing ';'
                    if (doDollarBlock) {
                        statements.add(s);
                    } else {
                        statements.add(s.substring(0, s.length() - 1));
                    }
                }
                current.setLength(0);
                doDollarBlock = false;
            }
        }

        String leftover = current.toString().trim();
        if (!leftover.isEmpty()) {
            statements.add(leftover);
        }

        return statements.toArray(new String[0]);
    }

    /**
     * Flush current token (word) and update state machine.
     * <p>
     * Rules:
     * - BEGIN increases beginDepth, except if it immediately follows an Oracle DECLARE section (same block).
     * - DECLARE increases beginDepth and enables "inDeclareSection" until the following BEGIN.
     * - END sets sawEndToken=true (actual depth decrement happens when encountering the ';' after END).
     */
    private static TokenState flushToken(
        StringBuilder token,
        int beginDepth,
        boolean sawEndToken,
        boolean inDeclareSection,
        int plsqlDdlDepth,
        boolean sawCreate,
        boolean sawOr,
        boolean sawReplace,
        String[] lastTokenHolder
    ) {
        if (token.isEmpty()) {
            return new TokenState(beginDepth, sawEndToken, inDeclareSection,
                plsqlDdlDepth, sawCreate, sawOr, sawReplace, lastTokenHolder[0]);
        }

        String t = token.toString().toUpperCase(Locale.ROOT);
        lastTokenHolder[0] = t;
        token.setLength(0);

        // Detect "CREATE [OR REPLACE] <plsql_object>" to enter PL/SQL DDL mode
        if ("CREATE".equals(t)) {
            sawCreate = true;
            sawOr = false;
            sawReplace = false;
        } else if (sawCreate && "OR".equals(t)) {
            sawOr = true;
        } else if (sawCreate && sawOr && "REPLACE".equals(t)) {
            sawReplace = true;
        } else if (sawCreate) {
            // t is the object type (or something else)
            if ("PACKAGE".equals(t) || "PROCEDURE".equals(t) || "FUNCTION".equals(t)
                || "TRIGGER".equals(t) || "TYPE".equals(t)) {
                // PACKAGE BODY is two tokens; we enter on PACKAGE already; BODY doesn't change anything.
                plsqlDdlDepth++;
            }
            sawCreate = false;
            sawOr = false;
            sawReplace = false;
        }

        if ("DECLARE".equals(t)) {
            beginDepth++;
            inDeclareSection = true;
            sawEndToken = false;
        } else if ("BEGIN".equals(t)) {
            if (inDeclareSection) {
                // DECLARE ... BEGIN is one single Oracle anonymous block, do not increment nesting here.
                inDeclareSection = false;
            } else {
                beginDepth++;
            }
            sawEndToken = false;
        } else if ("END".equals(t)) {
            sawEndToken = true;
        } else {
            // If we previously saw END, keep sawEndToken=true to support "END <label>;"
            // BUT cancel it for END IF / END LOOP / END CASE which should not close a BEGIN...END block.
            if (sawEndToken) {
                if ("IF".equals(t) || "LOOP".equals(t) || "CASE".equals(t)) {
                    sawEndToken = false;
                } else {
                    // label or other identifier after END => keep true until ';'
                    sawEndToken = true;
                }
            } else {
                sawEndToken = false;
            }
        }

        return new TokenState(beginDepth, sawEndToken, inDeclareSection,
            plsqlDdlDepth, sawCreate, sawOr, sawReplace, lastTokenHolder[0]);
    }

    private static boolean isValidDollarTag(String tag) {
        // "$$" is valid
        if ("$$".equals(tag)) return true;

        // "$tag$" where tag is [A-Za-z0-9_]+
        if (tag.length() < 3) return false;
        if (tag.charAt(0) != '$' || tag.charAt(tag.length() - 1) != '$') return false;

        for (int i = 1; i < tag.length() - 1; i++) {
            char ch = tag.charAt(i);
            if (!(Character.isLetterOrDigit(ch) || ch == '_')) return false;
        }
        return true;
    }

    private static boolean isWordChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    private record TokenState(
        int beginDepth,
        boolean sawEndToken,
        boolean inDeclareSection,
        int plsqlDdlDepth,
        boolean sawCreate,
        boolean sawOr,
        boolean sawReplace,
        String lastTokenUpper
    ) {
    }
}
