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
        boolean inLineComment = false;
        boolean inBlockComment = false;

        // Postgres dollar-quoted strings: $$...$$ or $tag$...$tag$
        boolean inDollarQuote = false;
        String dollarTag = null; // e.g. "$$" or "$json$"

        int beginDepth = 0;
        boolean sawEndToken = false;
        StringBuilder token = new StringBuilder();

        boolean doDollarBlock = false;
        String lastTokenUpper = null;

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

            // Start comments (only when not in a single quote)
            if (!inSingleQuote && c == '-' && next == '-') {
                // flush token
                if (!token.isEmpty()) {
                    String t = token.toString().toUpperCase(Locale.ROOT);
                    lastTokenUpper = t;
                    token.setLength(0);

                    if ("BEGIN".equals(t)) {
                        beginDepth++;
                        sawEndToken = false;
                    } else if ("END".equals(t)) {
                        sawEndToken = true;
                    } else {
                        sawEndToken = false;
                    }
                }

                inLineComment = true;
                current.append(c); // keep the comment text
                continue;
            }

            if (!inSingleQuote && c == '/' && next == '*') {
                // flush token
                if (!token.isEmpty()) {
                    String t = token.toString().toUpperCase(Locale.ROOT);
                    lastTokenUpper = t;
                    token.setLength(0);

                    if ("BEGIN".equals(t)) {
                        beginDepth++;
                        sawEndToken = false;
                    } else if ("END".equals(t)) {
                        sawEndToken = true;
                    } else {
                        sawEndToken = false;
                    }
                }

                inBlockComment = true;
                current.append(c); // keep the comment text
                continue;
            }

            // Start / end single-quoted strings (handle doubled quotes '')
            if (c == '\'') {
                // flush token
                if (!token.isEmpty()) {
                    String t = token.toString().toUpperCase(Locale.ROOT);
                    lastTokenUpper = t;
                    token.setLength(0);

                    if ("BEGIN".equals(t)) {
                        beginDepth++;
                        sawEndToken = false;
                    } else if ("END".equals(t)) {
                        sawEndToken = true;
                    } else {
                        sawEndToken = false;
                    }
                }

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

            // Start Postgres dollar-quoted string (only when not in a single quote)
            if (!inSingleQuote && c == '$') {
                // detect $tag$ (including $$)
                int closing = sql.indexOf('$', i + 1);
                if (closing != -1) {
                    String candidate = sql.substring(i, closing + 1); // "$$" or "$tag$"
                    // Valid tag: starts/ends with $, contains only letters/digits/underscore between
                    if (candidate.length() >= 2 && isValidDollarTag(candidate)) {
                        // flush token
                        if (!token.isEmpty()) {
                            String t = token.toString().toUpperCase(Locale.ROOT);
                            lastTokenUpper = t;
                            token.setLength(0);

                            if ("BEGIN".equals(t)) {
                                beginDepth++;
                                sawEndToken = false;
                            } else if ("END".equals(t)) {
                                sawEndToken = true;
                            } else {
                                sawEndToken = false;
                            }
                        }

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
            if (!inSingleQuote) {
                if (isWordChar(c)) {
                    token.append(c);
                } else {
                    if (!token.isEmpty()) {
                        String t = token.toString().toUpperCase(Locale.ROOT);
                        lastTokenUpper = t;
                        token.setLength(0);

                        if ("BEGIN".equals(t)) {
                            beginDepth++;
                            sawEndToken = false;
                        } else if ("END".equals(t)) {
                            sawEndToken = true;
                        } else {
                            sawEndToken = false;
                        }
                    }

                    if (c == ';') {
                        if (beginDepth > 0 && sawEndToken) {
                            beginDepth = Math.max(0, beginDepth - 1);
                            sawEndToken = false;

                            if (beginDepth == 0) {
                                statements.add(current.toString().trim());
                                current.setLength(0);
                                doDollarBlock = false;
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
            if (beginDepth == 0 && c == ';' && !inSingleQuote) {
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
}
