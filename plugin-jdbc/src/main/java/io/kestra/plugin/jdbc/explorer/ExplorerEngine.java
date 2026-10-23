package io.kestra.plugin.jdbc.explorer;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;

// What differs between the embedded engines that export a database file.
public interface ExplorerEngine {

    // Prefix of the endpoint names, for example `duckdb` for `duckdb-query`.
    String name();

    // Opens the exported file so that nothing can be written, and locks the connection settings.
    Connection openReadOnly(Path file) throws SQLException;

    // Refuses anything that is not a single read statement, with a message that says what is allowed.
    void validate(Connection connection, String sql) throws SQLException;

    // Rewrites a validated statement so that no cell is read in full, for engines that cannot cap values themselves.
    default String boundCells(Connection connection, String sql) throws SQLException {
        return sql;
    }

    // Query returning one row per column: schema, table, kind (TABLE or VIEW), column, column type.
    String catalogSql();
}
