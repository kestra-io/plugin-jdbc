package io.kestra.plugin.jdbc.sqlite;

import io.kestra.plugin.jdbc.explorer.AbstractReadOnlyExplorerEndpoint;
import io.kestra.plugin.jdbc.explorer.ExplorerEngine;

import org.sqlite.JDBC;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Opens the exported file with `mode=ro` and `PRAGMA query_only=ON`.
 * The sqlite-jdbc driver exposes no authorizer hook and SQLite cannot disable external access, so the statement
 * guard is a keyword allowlist: thinner than the DuckDB one.
 */
final class SqliteExplorerEngine implements ExplorerEngine {
    static final SqliteExplorerEngine INSTANCE = new SqliteExplorerEngine();

    private static final String READ_ONLY_MESSAGE = "Only a single read-only SELECT, WITH, EXPLAIN or PRAGMA table_info statement is allowed in the explorer.";
    private static final Set<String> FIRST_KEYWORDS = Set.of("select", "with", "explain", "pragma");
    private static final Pattern FORBIDDEN = Pattern.compile(
        "\\b(attach|detach|load_extension|vacuum|insert|update|delete|create|drop|alter|reindex|analyze|savepoint|release|begin|commit|rollback)\\b",
        Pattern.CASE_INSENSITIVE
    );
    private static final Pattern PRAGMA_TABLE_INFO = Pattern.compile("^\\s*pragma\\s+table_info\\s*\\(\\s*(?:\"\"|[A-Za-z_][A-Za-z0-9_]*)\\s*\\)\\s*;?\\s*$", Pattern.CASE_INSENSITIVE);

    private SqliteExplorerEngine() {
    }

    @Override
    public String name() {
        return "sqlite";
    }

    @Override
    public Connection openReadOnly(Path file) throws SQLException {
        var properties = new Properties();
        properties.setProperty("open_mode", "1");
        // Drivers of a plugin jar are not registered with DriverManager, so the driver is called directly.
        var connection = new JDBC().connect("jdbc:sqlite:file:" + file.toAbsolutePath() + "?mode=ro", properties);
        try (var statement = connection.createStatement()) {
            statement.execute("PRAGMA query_only=ON");
        } catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    @Override
    public void validate(Connection connection, String sql) {
        var code = AbstractReadOnlyExplorerEndpoint.stripLiteralsAndComments(sql).trim();
        var withoutTrailingSemicolon = code.endsWith(";") ? code.substring(0, code.length() - 1) : code;
        if (withoutTrailingSemicolon.isBlank() || withoutTrailingSemicolon.contains(";")) {
            throw new IllegalArgumentException(READ_ONLY_MESSAGE);
        }

        var first = withoutTrailingSemicolon.split("\\s+", 2)[0].toLowerCase(Locale.ROOT);
        if (!FIRST_KEYWORDS.contains(first)) {
            throw new IllegalArgumentException(READ_ONLY_MESSAGE);
        }
        if (first.equals("pragma") && !PRAGMA_TABLE_INFO.matcher(code).matches()) {
            throw new IllegalArgumentException("Only PRAGMA table_info(<table>) is allowed in the explorer, for example PRAGMA table_info(people).");
        }
        if (FORBIDDEN.matcher(withoutTrailingSemicolon).find()) {
            throw new IllegalArgumentException(READ_ONLY_MESSAGE);
        }
    }

    @Override
    public String catalogSql() {
        return """
            SELECT 'main', m.name, upper(m.type), p.name, p.type
            FROM sqlite_master m, pragma_table_info(m.name) p
            WHERE m.type IN ('table', 'view') AND m.name NOT LIKE 'sqlite_%'
            ORDER BY m.name, p.cid""";
    }
}
