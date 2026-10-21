package io.kestra.plugin.jdbc.explorer;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * What differs between the embedded engines that export a database file.
 */
public interface ExplorerEngine {

    /**
     * Prefix of the endpoint names, for example `duckdb` for `duckdb-catalog` and `duckdb-query`.
     */
    String name();

    /**
     * Opens the exported file so that nothing can be written, and locks the connection settings.
     */
    Connection openReadOnly(Path file) throws SQLException;

    /**
     * Refuses anything that is not a single read statement, with a message that tells what is allowed.
     *
     * @throws IllegalArgumentException when the statement is refused
     */
    void validate(Connection connection, String sql) throws SQLException;

    /**
     * Query returning one row per column with, in order: schema, table, kind (TABLE or VIEW), column, column type.
     */
    String catalogSql();
}
