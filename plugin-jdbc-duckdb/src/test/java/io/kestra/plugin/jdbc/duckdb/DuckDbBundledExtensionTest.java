package io.kestra.plugin.jdbc.duckdb;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
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

    // the community install fallback hides a bundled binary that fails to load whenever the network is up, so load the bundled file only
    @Test
    void shouldLoadBundledIonWithoutCommunityInstall(@TempDir Path tempDir) throws Exception {
        var platform = DuckDbConnectionSetup.bundledPlatform();
        assumeTrue(platform != null, "ion is only bundled for Linux");

        var extension = tempDir.resolve("ion.duckdb_extension");
        try (var input = getClass().getClassLoader().getResourceAsStream(DuckDbConnectionSetup.bundledResourcePath("ion", platform))) {
            assertThat(input, notNullValue());
            Files.copy(input, extension);
        }

        try (var connection = DriverManager.getConnection("jdbc:duckdb:");
             var statement = connection.createStatement()) {
            statement.execute("LOAD '%s';".formatted(extension.toAbsolutePath().toString().replace("'", "''")));

            try (var resultSet = statement.executeQuery("SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'ion'")) {
                assertThat(resultSet.next(), is(true));
                assertThat(resultSet.getBoolean(1), is(true));
            }
        }
    }
}
