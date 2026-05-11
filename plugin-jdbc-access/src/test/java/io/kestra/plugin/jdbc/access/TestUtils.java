package io.kestra.plugin.jdbc.access;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class TestUtils {

    public static final String DB_PATH = System.getProperty("java.io.tmpdir") + "/test_access.accdb";

    public static String url() {
        return "jdbc:ucanaccess://" + DB_PATH;
    }

    public static String username() {
        return "";
    }

    public static String password() {
        return "";
    }

    /**
     * Creates a fresh Access database file at the given path.
     * UCanAccess requires newDatabaseVersion when the file does not exist.
     * Deletes any existing file first to ensure a clean state.
     */
    public static void createDatabase(String dbPath) throws SQLException, IOException {
        Files.deleteIfExists(Path.of(dbPath));
        // newDatabaseVersion=V2010 instructs UCanAccess to create a new .accdb file
        // immediatelyReleaseResources ensures UCanAccess does not keep a cached in-memory mirror
        try (var conn = DriverManager.getConnection(
                "jdbc:ucanaccess://" + dbPath + ";newDatabaseVersion=V2010;immediatelyReleaseResources=true")) {
            conn.setAutoCommit(true);
        }
    }
}
