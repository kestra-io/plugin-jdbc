package io.kestra.plugin.jdbc.duckdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class DuckDbBundledExtensionTest {
    @Test
    void shouldBundleExtensionsBuiltForTheDriverDuckDbVersion() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT version()")) {
            resultSet.next();

            assertThat(DuckDbConnectionSetup.bundledDuckDbVersion()).isEqualTo(resultSet.getString(1));
        }
    }

    // The community install fallback hides a bundled binary that fails to load whenever the network is available, so load the bundled file only.
    @Test
    void shouldLoadBundledIonWithoutCommunityInstall(@TempDir Path tempDir) throws Exception {
        String platform = DuckDbConnectionSetup.bundledPlatform();
        assumeTrue(platform != null, "ion is only bundled for Linux");

        Path extension = tempDir.resolve("ion.duckdb_extension");
        try (InputStream inputStream = DuckDbBundledExtensionTest.class.getClassLoader()
            .getResourceAsStream(DuckDbConnectionSetup.bundledResourcePath("ion", platform))) {
            assertThat(inputStream).isNotNull();
            Files.copy(inputStream, extension);
        }

        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement()) {
            statement.execute("LOAD '" + extension.toAbsolutePath().toString().replace("'", "''") + "';");

            try (ResultSet resultSet = statement.executeQuery("SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'ion'")) {
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getBoolean(1)).isTrue();
            }
        }
    }
}
