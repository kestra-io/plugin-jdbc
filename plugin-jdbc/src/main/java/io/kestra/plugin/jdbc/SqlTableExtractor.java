package io.kestra.plugin.jdbc;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

// Best-effort tokenizer, not a SQL parser: CTEs, temporary tables and dialect quirks lower the confidence or are missed.
public final class SqlTableExtractor {
    private static final Set<String> FILE_READERS = Set.of(
        "read_parquet", "read_csv", "read_csv_auto", "read_json", "read_json_auto", "read_ndjson",
        "read_text", "read_blob", "parquet_scan", "csv_scan", "json_scan", "glob"
    );
    private static final Set<String> TABLE_MODIFIERS = Set.of(
        "temp", "temporary", "unlogged", "global", "local", "transient", "external", "volatile", "or", "replace", "virtual"
    );
    private static final Set<String> NON_TABLE_WORDS = Set.of(
        "select", "lateral", "unnest", "values", "only", "dual", "table", "set", "where", "group", "order", "limit"
    );
    private static final Set<String> FUNCTION_LIKE_PARENS = Set.of(
        "extract", "substring", "trim", "overlay", "position", "date_part"
    );
    private static final Set<String> STATEMENT_STARTS = Set.of(
        "select", "insert", "update", "delete", "merge", "create", "drop", "alter", "truncate", "copy", "replace"
    );
    private static final Pattern URL_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+.-]*://.*");
    private static final Pattern TYPE_PREFIX = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_]*:(?!//)");

    private SqlTableExtractor() {
    }

    public enum Access {
        READ,
        WRITE,
        CREATE,
        DROP
    }

    public enum Confidence {
        MEDIUM,
        LOW
    }

    public enum Kind {
        TABLE,
        FILE,
        URL
    }

    public enum Dialect {
        ANSI('"', false, false),
        MYSQL('`', true, false),
        SQLSERVER('[', false, false),
        DUCKDB('"', false, true);

        private final char identifierQuote;
        private final boolean backslashEscapes;
        private final boolean detectFiles;

        Dialect(char identifierQuote, boolean backslashEscapes, boolean detectFiles) {
            this.identifierQuote = identifierQuote;
            this.backslashEscapes = backslashEscapes;
            this.detectFiles = detectFiles;
        }

        public static Dialect of(DatabaseMetaData metaData) {
            try {
                var product = metaData.getDatabaseProductName().toLowerCase(Locale.ROOT);
                if (product.contains("duckdb")) {
                    return DUCKDB;
                }
                if (product.contains("sql server") || product.contains("access") || product.contains("sybase") || product.contains("adaptive server")) {
                    return SQLSERVER;
                }
                if ("`".equals(metaData.getIdentifierQuoteString())) {
                    return MYSQL;
                }
            } catch (SQLException | RuntimeException ignored) {
                // Drivers without metadata support fall back to ANSI quoting.
            }
            return ANSI;
        }
    }

    // Literal and comment rules come from the task, which knows its database better than the product name does.
    public record Syntax(boolean backslashEscapes, boolean hashLineComments) {
        static Syntax of(Dialect dialect) {
            return new Syntax(dialect.backslashEscapes, dialect == Dialect.MYSQL);
        }
    }

    @Getter
    @Builder
    public static class TableReference {
        @Schema(title = "Table name", description = "For DuckDB file reads, the file path or URL.")
        private final String name;

        @Schema(title = "Schema name", description = "Null when the statement does not qualify the table with a schema.")
        private final String schema;

        @Schema(title = "Database name", description = "Null when the statement does not qualify the table with a database.")
        private final String database;

        @Schema(
            title = "How the statement uses the table",
            description = "One of READ, WRITE, CREATE or DROP."
        )
        private final Access access;

        @Schema(
            title = "How reliable the detection is",
            description = "MEDIUM when detected from the SQL text, LOW for temporary tables and ambiguous names."
        )
        private final Confidence confidence;

        @Schema(
            title = "Kind of source",
            description = "Only set on DuckDB: TABLE, FILE or URL (files read through `read_parquet`, `read_csv`, `ATTACH` and quoted paths)."
        )
        private final Kind kind;

        // Quoted identifiers keep their case, unquoted ones are folded by the database. Not part of the output.
        @Getter(AccessLevel.NONE)
        private final boolean quotedName;
        @Getter(AccessLevel.NONE)
        private final boolean quotedSchema;
        @Getter(AccessLevel.NONE)
        private final boolean quotedDatabase;

        private TableReference(
            String name, String schema, String database, Access access, Confidence confidence, Kind kind,
            boolean quotedName, boolean quotedSchema, boolean quotedDatabase
        ) {
            this.name = name;
            this.schema = schema;
            this.database = database;
            this.access = access;
            this.confidence = confidence;
            this.kind = kind;
            this.quotedName = quotedName;
            this.quotedSchema = quotedSchema;
            this.quotedDatabase = quotedDatabase;
        }

        boolean isQuotedName() {
            return quotedName;
        }

        boolean isQuotedSchema() {
            return quotedSchema;
        }

        boolean isQuotedDatabase() {
            return quotedDatabase;
        }

        private String key() {
            return String.join("|", String.valueOf(database), String.valueOf(schema), name, String.valueOf(access), String.valueOf(kind));
        }
    }

    private enum TokenType {
        WORD,
        QUOTED,
        STRING,
        SYMBOL,
        VARIABLE
    }

    private record Token(TokenType type, String text) {
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

    public static List<TableReference> extract(String sql, Dialect dialect) {
        return extract(sql, dialect, Syntax.of(dialect));
    }

    public static List<TableReference> extract(String sql, Dialect dialect, Syntax syntax) {
        if (sql == null || sql.isBlank()) {
            return List.of();
        }

        var tokens = tokenize(sql, dialect, syntax);
        var cteNames = collectCteNames(tokens);
        var found = new LinkedHashMap<String, TableReference>();
        new Walker(tokens, dialect, cteNames, found).walk();
        return List.copyOf(found.values());
    }

    // The user-info ends at the last `@`; when a `?` or `#` comes before it the split is ambiguous, so nothing is kept.
    static String safeUrl(String url) {
        var schemeEnd = url.indexOf("://");
        var scheme = url.substring(0, schemeEnd + 3);
        var rest = url.substring(schemeEnd + 3);
        var at = rest.lastIndexOf('@');
        var cut = indexOfAny(rest, '?', '#');
        if (at >= 0 && cut >= 0 && cut < at) {
            return scheme + "***";
        }
        rest = rest.substring(at + 1);
        cut = indexOfAny(rest, '?', '#');
        return scheme + (cut < 0 ? rest : rest.substring(0, cut));
    }

    private static int indexOfAny(String text, char first, char second) {
        for (var i = 0; i < text.length(); i++) {
            if (text.charAt(i) == first || text.charAt(i) == second) {
                return i;
            }
        }
        return -1;
    }

    public static String statementKind(String sql) {
        if (sql == null || sql.isBlank()) {
            return "OTHER";
        }
        var tokens = tokenize(sql, Dialect.ANSI, Syntax.of(Dialect.ANSI));
        var depth = 0;
        var first = true;
        for (var token : tokens) {
            if (token.isSymbol("(")) {
                depth++;
            } else if (token.isSymbol(")")) {
                depth--;
            } else if (token.type() == TokenType.WORD && depth == 0) {
                var word = token.lower();
                if (first && !word.equals("with")) {
                    return STATEMENT_STARTS.contains(word) ? word.toUpperCase(Locale.ROOT) : "OTHER";
                }
                first = false;
                if (STATEMENT_STARTS.contains(word) && !word.equals("replace")) {
                    return word.toUpperCase(Locale.ROOT);
                }
            }
        }
        return "OTHER";
    }

    private static Set<String> collectCteNames(List<Token> tokens) {
        var names = new HashSet<String>();
        for (var i = 0; i < tokens.size(); i++) {
            if (!tokens.get(i).isWord("with")) {
                continue;
            }
            var j = i + 1;
            if (j < tokens.size() && tokens.get(j).isWord("recursive")) {
                j++;
            }
            while (j < tokens.size()) {
                var nameToken = tokens.get(j);
                if (nameToken.type() != TokenType.WORD && nameToken.type() != TokenType.QUOTED) {
                    break;
                }
                j++;
                if (j < tokens.size() && tokens.get(j).isSymbol("(")) {
                    j = skipParens(tokens, j);
                }
                if (j >= tokens.size() || !tokens.get(j).isWord("as")) {
                    break;
                }
                names.add(nameToken.lower());
                j++;
                if (j < tokens.size() && tokens.get(j).isWord("not")) {
                    j++;
                }
                if (j < tokens.size() && tokens.get(j).isWord("materialized")) {
                    j++;
                }
                if (j >= tokens.size() || !tokens.get(j).isSymbol("(")) {
                    break;
                }
                j = skipParens(tokens, j);
                if (j < tokens.size() && tokens.get(j).isSymbol(",")) {
                    j++;
                } else {
                    break;
                }
            }
        }
        return names;
    }

    // Returns the index right after the parenthesis matching the one at `open`.
    private static int skipParens(List<Token> tokens, int open) {
        var depth = 0;
        for (var i = open; i < tokens.size(); i++) {
            if (tokens.get(i).isSymbol("(")) {
                depth++;
            } else if (tokens.get(i).isSymbol(")")) {
                depth--;
                if (depth == 0) {
                    return i + 1;
                }
            }
        }
        return tokens.size();
    }

    private static final class Walker {
        private final List<Token> tokens;
        private final Dialect dialect;
        private final Set<String> cteNames;
        private final LinkedHashMap<String, TableReference> found;
        private final Deque<Boolean> functionParens = new ArrayDeque<>();
        private int index;
        private boolean inMerge;
        private boolean inDelete;

        private Walker(List<Token> tokens, Dialect dialect, Set<String> cteNames, LinkedHashMap<String, TableReference> found) {
            this.tokens = tokens;
            this.dialect = dialect;
            this.cteNames = cteNames;
            this.found = found;
        }

        void walk() {
            for (index = 0; index < tokens.size(); index++) {
                var token = tokens.get(index);
                if (token.isSymbol("(")) {
                    var previous = peek(-1);
                    functionParens.push(previous != null && previous.type() == TokenType.WORD && FUNCTION_LIKE_PARENS.contains(previous.lower()));
                    continue;
                }
                if (token.isSymbol(")")) {
                    if (!functionParens.isEmpty()) {
                        functionParens.pop();
                    }
                    continue;
                }
                if (token.isSymbol(";")) {
                    inMerge = false;
                    inDelete = false;
                    continue;
                }
                if (token.type() == TokenType.WORD) {
                    visitKeyword(token);
                }
            }
        }

        private void visitKeyword(Token token) {
            var word = token.lower();
            switch (word) {
                case "from" -> {
                    if (isInFunctionParens() || previousIs("distinct")) {
                        return;
                    }
                    var access = previousIs("delete") ? Access.WRITE : Access.READ;
                    readTableList(index + 1, access, access == Access.READ);
                }
                case "join" -> readTableList(index + 1, Access.READ, false);
                case "into" -> {
                    var start = index + 1;
                    if (peekIs(start, "table")) {
                        start++;
                    }
                    if (peekIs(start, "function")) {
                        return;
                    }
                    readTableList(start, Access.WRITE, false);
                }
                case "update" -> {
                    if (previousIs("for", "on", "do", "then", "key") || peekIs(index + 1, "set")) {
                        return;
                    }
                    readTableList(index + 1, Access.WRITE, false);
                }
                case "merge" -> {
                    inMerge = true;
                }
                case "delete" -> inDelete = true;
                case "using" -> {
                    if (inMerge || inDelete) {
                        var next = peek(1);
                        if (next != null && !next.isSymbol("(")) {
                            readTableList(index + 1, Access.READ, inDelete);
                        }
                    }
                }
                case "truncate" -> {
                    var start = index + 1;
                    if (peekIs(start, "table")) {
                        start++;
                    }
                    readTableList(start, Access.WRITE, true);
                }
                case "drop" -> {
                    if (peekIs(index + 1, "table")) {
                        var start = index + 2;
                        if (peekIs(start, "if") && peekIs(start + 1, "exists")) {
                            start += 2;
                        }
                        readTableList(start, Access.DROP, true);
                    }
                }
                case "create" -> readCreate();
                case "attach" -> {
                    if (dialect.detectFiles) {
                        var next = peek(1);
                        if (next != null && next.type() == TokenType.WORD && next.lower().equals("database")) {
                            next = peek(2);
                        }
                        if (next != null && next.type() == TokenType.WORD && next.lower().equals("if")) {
                            next = peek(4);
                        }
                        if (next != null && next.type() == TokenType.STRING) {
                            addFile(next.text, true);
                        }
                    }
                }
                default -> {
                }
            }
        }

        private void readCreate() {
            var cursor = index + 1;
            var temporary = false;
            while (cursor < tokens.size() && tokens.get(cursor).type() == TokenType.WORD && TABLE_MODIFIERS.contains(tokens.get(cursor).lower())) {
                temporary |= Set.of("temp", "temporary", "volatile").contains(tokens.get(cursor).lower());
                cursor++;
            }
            if (!peekIs(cursor, "table")) {
                return;
            }
            cursor++;
            if (peekIs(cursor, "if") && peekIs(cursor + 1, "not") && peekIs(cursor + 2, "exists")) {
                cursor += 3;
            }
            var reference = readQualifiedName(cursor, true);
            if (reference != null) {
                add(reference, Access.CREATE, temporary ? Confidence.LOW : null, null);
            }
        }

        // Reads `name [alias] [, name [alias]]*`. The list form is only followed for FROM, TRUNCATE and DROP.
        private void readTableList(int start, Access access, boolean allowList) {
            var cursor = start;
            while (cursor < tokens.size()) {
                var token = tokens.get(cursor);
                if (dialect.detectFiles && token.type() == TokenType.STRING && access == Access.READ) {
                    addFile(token.text, false);
                    return;
                }
                if (token.type() == TokenType.WORD && FILE_READERS.contains(token.lower()) && peekIs(cursor + 1, "(")) {
                    var argument = tokenAt(cursor + 2);
                    if (dialect.detectFiles && argument != null && argument.type() == TokenType.STRING) {
                        addFile(argument.text, false);
                    }
                    return;
                }
                var reference = readQualifiedName(cursor, access != Access.READ);
                if (reference == null) {
                    return;
                }
                add(reference, access, null, null);
                cursor = reference.next;
                if (!allowList) {
                    return;
                }
                cursor = skipAlias(cursor);
                if (cursor < tokens.size() && tokens.get(cursor).isSymbol(",")) {
                    cursor++;
                } else {
                    return;
                }
            }
        }

        private int skipAlias(int cursor) {
            if (cursor < tokens.size() && tokens.get(cursor).isWord("as")) {
                cursor++;
            }
            if (cursor < tokens.size()
                && (tokens.get(cursor).type() == TokenType.QUOTED
                || (tokens.get(cursor).type() == TokenType.WORD && !isClauseKeyword(tokens.get(cursor).lower())))) {
                cursor++;
            }
            return cursor;
        }

        private boolean isClauseKeyword(String word) {
            return Set.of(
                "where", "group", "order", "limit", "join", "inner", "left", "right", "full", "cross", "natural", "on",
                "using", "union", "intersect", "except", "having", "window", "offset", "fetch", "for", "set", "values",
                "select", "final", "format", "settings", "returning", "when", "then"
            ).contains(word);
        }

        private record QualifiedName(List<String> parts, List<Boolean> quoted, int next) {
        }

        private QualifiedName readQualifiedName(int start, boolean target) {
            var parts = new ArrayList<String>();
            var quoted = new ArrayList<Boolean>();
            var cursor = start;
            while (cursor < tokens.size()) {
                var token = tokens.get(cursor);
                if (token.type() == TokenType.WORD) {
                    if (parts.isEmpty() && NON_TABLE_WORDS.contains(token.lower())) {
                        return null;
                    }
                    parts.add(token.text);
                    quoted.add(false);
                } else if (token.type() == TokenType.QUOTED) {
                    parts.add(token.text);
                    quoted.add(true);
                } else if (token.type() == TokenType.VARIABLE && parts.isEmpty() && token.text.startsWith("#")) {
                    parts.add(token.text);
                    quoted.add(false);
                } else {
                    break;
                }
                cursor++;
                if (cursor < tokens.size() && tokens.get(cursor).isSymbol(".") && cursor + 1 < tokens.size()) {
                    cursor++;
                    if (tokens.get(cursor).isSymbol(".")) {
                        // SQL Server `db..table`: the schema is omitted.
                        parts.add(null);
                        quoted.add(false);
                        cursor++;
                    }
                    continue;
                }
                break;
            }
            var isFunctionCall = !target && parts.size() == 1 && tokens.get(start).type() == TokenType.WORD
                && peekIs(cursor, "(");
            if (parts.isEmpty() || isFunctionCall) {
                return null;
            }
            return new QualifiedName(parts, quoted, cursor);
        }

        private void add(QualifiedName qualified, Access access, Confidence forced, Kind kind) {
            var parts = new ArrayList<>(qualified.parts);
            var quoted = new ArrayList<>(qualified.quoted);
            while (parts.size() > 3) {
                parts.removeFirst();
                quoted.removeFirst();
            }
            var name = parts.getLast();
            if (name == null) {
                return;
            }
            var schema = parts.size() >= 2 ? parts.get(parts.size() - 2) : null;
            var database = parts.size() == 3 ? parts.getFirst() : null;
            var quotedSchema = parts.size() >= 2 && quoted.get(parts.size() - 2);
            var quotedDatabase = parts.size() == 3 && quoted.getFirst();
            var confidence = forced != null ? forced : Confidence.MEDIUM;

            if (schema == null && database == null && cteNames.contains(name.toLowerCase(Locale.ROOT)) && access == Access.READ) {
                return;
            }
            if (name.startsWith("#") || name.startsWith("@")) {
                confidence = Confidence.LOW;
            }
            var reference = TableReference.builder()
                .name(name)
                .schema(schema)
                .database(database)
                .access(access)
                .confidence(confidence)
                .kind(dialect.detectFiles ? (kind == null ? Kind.TABLE : kind) : null)
                .quotedName(quoted.getLast())
                .quotedSchema(quotedSchema)
                .quotedDatabase(quotedDatabase)
                .build();
            found.putIfAbsent(reference.key(), reference);
        }

        // Literals can carry secrets: connection strings are never output and URLs lose user-info, query and fragment.
        private void addFile(String literal, boolean attach) {
            if (attach && (literal.contains("=") || TYPE_PREFIX.matcher(literal).find())) {
                return;
            }
            var isUrl = URL_PATTERN.matcher(literal).matches();
            var reference = TableReference.builder()
                .name(isUrl ? safeUrl(literal) : literal)
                .access(Access.READ)
                .confidence(Confidence.MEDIUM)
                .kind(isUrl ? Kind.URL : Kind.FILE)
                .build();
            found.putIfAbsent(reference.key(), reference);
        }

        private boolean isInFunctionParens() {
            return functionParens.stream().anyMatch(Boolean::booleanValue);
        }

        private Token peek(int offset) {
            return tokenAt(index + offset);
        }

        private Token tokenAt(int position) {
            return position >= 0 && position < tokens.size() ? tokens.get(position) : null;
        }

        private boolean peekIs(int position, String text) {
            if (position < 0 || position >= tokens.size()) {
                return false;
            }
            var token = tokens.get(position);
            return token.isWord(text) || token.isSymbol(text);
        }

        private boolean previousIs(String... words) {
            var previous = peek(-1);
            if (previous == null || previous.type() != TokenType.WORD) {
                return false;
            }
            for (var word : words) {
                if (previous.isWord(word)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static List<Token> tokenize(String sql, Dialect dialect, Syntax syntax) {
        var tokens = new ArrayList<Token>();
        var length = sql.length();
        var i = 0;

        while (i < length) {
            var c = sql.charAt(i);
            var next = i + 1 < length ? sql.charAt(i + 1) : '\0';

            if (Character.isWhitespace(c)) {
                i++;
            } else if (c == '-' && next == '-') {
                i = skipLine(sql, i);
            } else if (c == '#' && syntax.hashLineComments()) {
                i = skipLine(sql, i);
            } else if (c == '/' && next == '*') {
                var end = sql.indexOf("*/", i + 2);
                i = end < 0 ? length : end + 2;
            } else if (c == '$' && isDollarQuoteStart(sql, i)) {
                var endTag = sql.indexOf('$', i + 1);
                var tag = sql.substring(i, endTag + 1);
                var close = sql.indexOf(tag, endTag + 1);
                i = close < 0 ? length : close + tag.length();
            } else if (c == '\'') {
                var escapes = syntax.backslashEscapes() || (i > 0 && (sql.charAt(i - 1) == 'E' || sql.charAt(i - 1) == 'e'));
                var end = scanQuoted(sql, i, '\'', escapes);
                tokens.add(new Token(TokenType.STRING, unquote(sql, i, end, '\'')));
                i = end;
            } else if (c == '"' && dialect == Dialect.MYSQL) {
                i = scanQuoted(sql, i, '"', true);
            } else if (c == '"') {
                var end = scanQuoted(sql, i, '"', false);
                tokens.add(new Token(TokenType.QUOTED, unquote(sql, i, end, '"')));
                i = end;
            } else if (c == '`') {
                var end = scanQuoted(sql, i, '`', false);
                tokens.add(new Token(TokenType.QUOTED, unquote(sql, i, end, '`')));
                i = end;
            } else if (c == '[' && dialect == Dialect.SQLSERVER) {
                var end = sql.indexOf(']', i + 1);
                end = end < 0 ? length : end + 1;
                tokens.add(new Token(TokenType.QUOTED, sql.substring(i + 1, Math.max(i + 1, end - 1))));
                i = end;
            } else if (Character.isLetter(c) || c == '_' || c == '#' || c == '@') {
                var start = i;
                i++;
                while (i < length && isWordChar(sql.charAt(i))) {
                    i++;
                }
                var text = sql.substring(start, i);
                tokens.add(new Token(c == '#' || c == '@' ? TokenType.VARIABLE : TokenType.WORD, text));
            } else if (Character.isDigit(c)) {
                i++;
                while (i < length && (Character.isLetterOrDigit(sql.charAt(i)) || sql.charAt(i) == '.')) {
                    i++;
                }
            } else {
                tokens.add(new Token(TokenType.SYMBOL, String.valueOf(c)));
                i++;
            }
        }
        return tokens;
    }

    private static boolean isWordChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$' || c == '#';
    }

    private static boolean isDollarQuoteStart(String sql, int start) {
        var end = start + 1;
        while (end < sql.length() && (Character.isLetterOrDigit(sql.charAt(end)) || sql.charAt(end) == '_')) {
            end++;
        }
        var isTag = end < sql.length() && sql.charAt(end) == '$';
        var isPositionalParam = end > start + 1 && Character.isDigit(sql.charAt(start + 1));
        return isTag && !isPositionalParam && (start == 0 || !isWordChar(sql.charAt(start - 1)));
    }

    private static int skipLine(String sql, int start) {
        var end = sql.indexOf('\n', start);
        return end < 0 ? sql.length() : end + 1;
    }

    // Returns the index right after the closing quote; doubled quotes are escapes.
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
