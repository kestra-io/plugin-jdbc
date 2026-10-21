package io.kestra.plugin.jdbc.duckdb;

import io.kestra.plugin.jdbc.explorer.AbstractReadOnlyExplorerEndpoint;
import io.kestra.plugin.jdbc.explorer.ExplorerEngine;

import org.duckdb.DuckDBDriver;

import java.nio.file.Path;
import java.sql.Connection;
import java.util.Properties;
import java.sql.SQLException;

/**
 * Opens the exported file attached `READ_ONLY` to a fresh in-memory database, then locks the configuration
 * so the user SQL cannot lift the limits or reach the filesystem and the network.
 */
final class DuckDbExplorerEngine implements ExplorerEngine {
    static final DuckDbExplorerEngine INSTANCE = new DuckDbExplorerEngine();

    private static final String READ_ONLY_MESSAGE = "Only a single read-only SELECT statement is allowed in the explorer.";

    private DuckDbExplorerEngine() {
    }

    @Override
    public String name() {
        return "duckdb";
    }

    @Override
    public Connection openReadOnly(Path file) throws SQLException {
        // Drivers of a plugin jar are not registered with DriverManager, so the driver is called directly.
        var connection = new DuckDBDriver().connect("jdbc:duckdb:", new Properties());
        try (var statement = connection.createStatement()) {
            statement.execute("ATTACH '" + file.toAbsolutePath().toString().replace("'", "''") + "' AS explorer (READ_ONLY)");
            statement.execute("USE explorer");
            statement.execute("SET autoinstall_known_extensions=false");
            statement.execute("SET autoload_known_extensions=false");
            statement.execute("SET memory_limit='512MB'");
            statement.execute("SET threads=2");
            statement.execute("SET enable_external_access=false");
            statement.execute("SET lock_configuration=true");
        } catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    // json_serialize_sql only serializes a single SELECT: anything else, including several statements, comes back as an error.
    @Override
    public void validate(Connection connection, String sql) throws SQLException {
        // json_serialize_sql only looks at the first statement, so a second one has to be refused here.
        var code = AbstractReadOnlyExplorerEndpoint.stripLiteralsAndComments(sql).trim();
        var firstSemicolon = code.indexOf(';');
        if (firstSemicolon >= 0 && !code.substring(firstSemicolon + 1).replace(";", "").isBlank()) {
            throw new IllegalArgumentException(READ_ONLY_MESSAGE);
        }
        // The function needs a constant argument, so the statement is passed as an escaped literal.
        try (var statement = connection.createStatement();
             var rs = statement.executeQuery("SELECT json_serialize_sql('" + sql.replace("'", "''") + "')")) {
            if (!rs.next() || rs.getString(1) == null || rs.getString(1).contains("\"error\":true")) {
                throw new IllegalArgumentException(READ_ONLY_MESSAGE);
            }
        }
    }

    @Override
    public String catalogSql() {
        return """
            SELECT c.schema_name, c.table_name,
                   CASE WHEN v.view_name IS NULL THEN 'TABLE' ELSE 'VIEW' END,
                   c.column_name, c.data_type
            FROM duckdb_columns() c
            LEFT JOIN duckdb_views() v
              ON v.database_name = c.database_name AND v.schema_name = c.schema_name AND v.view_name = c.table_name
            WHERE c.database_name = 'explorer' AND NOT c.internal
            ORDER BY c.schema_name, c.table_name, c.column_index""";
    }
}
