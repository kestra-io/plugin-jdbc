package io.kestra.plugin.jdbc.duckdb;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.spec.X509EncodedKeySpec;
import java.sql.DriverManager;
import java.util.Base64;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class DuckDbBundledExtensionTest {
    @Test
    void shouldBundleExtensionsBuiltForTheDriverDuckDbVersion() throws Exception {
        try (var connection = DriverManager.getConnection("jdbc:duckdb:");
             var resultSet = connection.createStatement().executeQuery("SELECT version()")) {
            resultSet.next();

            assertThat(DuckDbConnectionSetup.bundledDuckDbVersion(), is(resultSet.getString(1)));
        }
    }

    @Test
    void shouldBundleValidCommunityPublicKeys() throws Exception {
        var keys = DuckDbConnectionSetup.bundledPublicKeys();
        assertThat(keys, not(empty()));
        for (String pem : keys) {
            var body = pem.replace("-----BEGIN PUBLIC KEY-----", "")
                .replace("-----END PUBLIC KEY-----", "").replaceAll("\\s", "");
            assertThat(KeyFactory.getInstance("RSA").generatePublic(
                new X509EncodedKeySpec(Base64.getDecoder().decode(body))
            ).getAlgorithm(), is("RSA"));
        }
    }

    // Exercise the production bundled loader without the online fallback.
    @Test
    void shouldLoadBundledIonWithoutCommunityInstall(@TempDir Path tempDir) throws Exception {
        var platform = DuckDbConnectionSetup.bundledPlatform();
        assumeTrue(platform != null, "ion is only bundled for Linux");

        var taskDirectory = Files.createDirectories(tempDir.resolve("worker's task"));
        var properties = new Properties();
        properties.setProperty("extension_directory", taskDirectory.resolve(".duckdb_extensions/installed").toString());
        DuckDbConnectionSetup.configureConnectionProperties(properties, taskDirectory);
        var installed = Path.of(properties.getProperty("extension_directory"))
            .resolve(DuckDbConnectionSetup.bundledDuckDbVersion()).resolve(platform)
            .resolve("repositories/kestra_bundled/ion.duckdb_extension");

        try (var connection = DriverManager.getConnection("jdbc:duckdb:", properties);
             var statement = connection.createStatement()) {
            assertThat(DuckDbConnectionSetup.loadBundledExtension(connection, taskDirectory, "ion", platform), is(true));

            // The bundled install must not leave extension repositories open for the flow author's SQL.
            try (var result = statement.executeQuery("SELECT current_setting('allow_extension_repositories')")) {
                assertThat(result.next(), is(true));
                assertThat(result.getString(1), is("forbidden"));
            }

            try (var resultSet = statement.executeQuery("SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'ion'")) {
                assertThat(resultSet.next(), is(true));
                assertThat(resultSet.getBoolean("loaded"), is(true));
            }
            // Inspect the registry and file because 2.0 leaves install metadata empty.
            try (var result = statement.executeQuery("""
                SELECT prefix, key_fingerprints FROM duckdb_extension_repositories()
                WHERE repository_name = 'kestra_bundled'
                """)) {
                assertThat(result.next(), is(true));
                assertThat(Path.of(result.getString("prefix")).startsWith(taskDirectory), is(true));
                assertThat(result.getString("key_fingerprints"), not(emptyOrNullString()));
            }
            assertThat(Files.isRegularFile(installed), is(true));
            try (var result = statement.executeQuery("SELECT current_setting('allow_unsigned_extensions')")) {
                assertThat(result.next(), is(true));
                assertThat(result.getBoolean(1), is(false));
            }

            var ionFile = tempDir.resolve("sample.ion");
            Files.writeString(ionFile, "{a: 1, b: \"x\"}\n");
            try (var resultSet = statement.executeQuery("SELECT a, b FROM read_ion('%s')".formatted(
                ionFile.toAbsolutePath().toString().replace("'", "''")
            ))) {
                assertThat(resultSet.next(), is(true));
                assertThat(resultSet.getInt("a"), is(1));
                assertThat(resultSet.getString("b"), is("x"));
                assertThat(resultSet.next(), is(false));
            }
        }

        // A fresh session must reject an installed file with a modified signature.
        try (var file = new java.io.RandomAccessFile(installed.toFile(), "rw")) {
            int firstByte = file.read();
            file.seek(0);
            file.write(firstByte ^ 1);
        }
        try (var connection = DriverManager.getConnection("jdbc:duckdb:", properties);
             var statement = connection.createStatement()) {
            var failure = assertThrows(java.sql.SQLException.class,
                () -> statement.execute("LOAD ion FROM kestra_bundled"));
            assertThat(failure.getMessage(), containsString("signature"));
        }
    }

    @Test
    void shouldIsolateBundledInstallsAndRestoreWorkerCache(@TempDir Path directory) throws Exception {
        var platform = DuckDbConnectionSetup.bundledPlatform();
        assumeTrue(platform != null, "ion is only bundled for Linux");
        var workerCache = Files.createDirectories(directory.resolve("worker-cache"));
        var ion = directory.resolve("sample.ion");
        Files.writeString(ion, "{id: 42}\n");

        for (var index = 0; index < 2; index++) {
            var taskDirectory = directory.resolve("task-" + index);
            var properties = new Properties();
            properties.setProperty("extension_directory", workerCache.toString());
            DuckDbConnectionSetup.configureConnectionProperties(properties, taskDirectory);
            try (var connection = DriverManager.getConnection("jdbc:duckdb:", properties);
                 var statement = connection.createStatement()) {
                assertThat(DuckDbConnectionSetup.loadBundledExtension(connection, taskDirectory, "ion", platform), is(true));
                try (var result = statement.executeQuery("SELECT current_setting('extension_directory')")) {
                    assertThat(result.next(), is(true));
                    assertThat(Path.of(result.getString(1)), is(workerCache));
                }
                var installed = taskDirectory.resolve(".duckdb_extensions/installed")
                    .resolve(DuckDbConnectionSetup.bundledDuckDbVersion()).resolve(platform)
                    .resolve("repositories/kestra_bundled/ion.duckdb_extension");
                assertThat(Files.isRegularFile(installed), is(true));
                try (var result = statement.executeQuery("SELECT id FROM read_ion('%s')".formatted(
                    ion.toString().replace("'", "''")
                ))) {
                    assertThat(result.next(), is(true));
                    assertThat(result.getInt(1), is(42));
                }
            }
        }
    }

    @Test
    void shouldPreserveWorkerExtensionCache(@TempDir Path directory) throws Exception {
        var defaults = new Properties();
        DuckDbConnectionSetup.configureConnectionProperties(defaults, directory.resolve("default-task"));
        assertThat(defaults.containsKey("extension_directory"), is(false));

        var cache = Files.createDirectories(directory.resolve("worker-cache"));
        var properties = new Properties();
        properties.setProperty("extension_directory", cache.toString());
        DuckDbConnectionSetup.configureConnectionProperties(properties, directory.resolve("configured-task"));
        try (var connection = DriverManager.getConnection("jdbc:duckdb:", properties);
             var statement = connection.createStatement();
             var result = statement.executeQuery("SELECT current_setting('extension_directory')")) {
            assertThat(result.next(), is(true));
            assertThat(Path.of(result.getString(1)), is(cache));
        }
    }

    @Test
    void shouldRespectForbiddenRepositories(@TempDir Path tempDir) throws Exception {
        var platform = DuckDbConnectionSetup.bundledPlatform();
        assumeTrue(platform != null, "ion is only bundled for Linux");
        var properties = new Properties();
        DuckDbConnectionSetup.configureConnectionProperties(properties, tempDir);
        properties.setProperty("allow_extension_repositories", "forbidden");
        try (var connection = DriverManager.getConnection("jdbc:duckdb:", properties)) {
            assertThrows(java.sql.SQLException.class,
                () -> DuckDbConnectionSetup.loadBundledExtension(connection, tempDir, "ion", platform));
        }
    }
}
