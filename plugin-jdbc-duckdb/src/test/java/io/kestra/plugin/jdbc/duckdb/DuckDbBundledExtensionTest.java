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

    // Calls the production bundled loader without its online fallback. Also run
    // this test in a network-disabled container to establish offline behavior.
    @Test
    void shouldLoadBundledIonWithoutCommunityInstall(@TempDir Path tempDir) throws Exception {
        var platform = DuckDbConnectionSetup.bundledPlatform();
        assumeTrue(platform != null, "ion is only bundled for Linux");

        var taskDirectory = Files.createDirectories(tempDir.resolve("worker's task"));
        var properties = new Properties();
        DuckDbConnectionSetup.configureConnectionProperties(properties, taskDirectory);
        Path installed = Path.of(properties.getProperty("extension_directory"))
            .resolve(DuckDbConnectionSetup.bundledDuckDbVersion()).resolve(platform)
            .resolve("repositories/kestra_bundled/ion.duckdb_extension");

        try (var connection = DriverManager.getConnection("jdbc:duckdb:", properties);
             var statement = connection.createStatement()) {
            assertThat(DuckDbConnectionSetup.loadBundledExtension(connection, taskDirectory, "ion", platform), is(true));

            try (var resultSet = statement.executeQuery("SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'ion'")) {
                assertThat(resultSet.next(), is(true));
                assertThat(resultSet.getBoolean("loaded"), is(true));
            }
            // 2.0's duckdb_extensions() leaves installed_from/install_path empty
            // for this repository; inspect the registry and installed file instead.
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

        // A subsequent session must still verify the installed artifact against
        // the pinned keys, rather than trusting its local path or cached metadata.
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
