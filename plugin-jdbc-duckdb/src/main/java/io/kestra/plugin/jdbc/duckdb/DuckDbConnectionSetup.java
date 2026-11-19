package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Pattern;

final class DuckDbConnectionSetup {
    private static final Pattern EXTENSION_NAME = Pattern.compile("[A-Za-z0-9_]+");
    private static final String BUNDLED_EXTENSIONS_ROOT = "duckdb-community-extensions";
    private static final String BUNDLED_EXTENSIONS_METADATA = "duckdb-community-extensions.properties";
    private static final String BUNDLED_PUBLIC_KEYS = "duckdb-community-public-keys.pem";
    private static final String BUNDLED_REPOSITORY = "kestra_bundled";
    private static final Properties BUNDLED_EXTENSION_PROPERTIES = loadBundledExtensionProperties();

    private DuckDbConnectionSetup() {
    }

    static void configureConnectionProperties(Properties properties, Path workingDirectory) throws IOException {
        Path base = workingDirectory.resolve(".duckdb_extensions");
        // Repository definitions can only be relocated at connection startup.
        properties.putIfAbsent("extension_repository_directory", Files.createDirectories(base.resolve("repositories")).toString());
        properties.putIfAbsent("extension_directory", Files.createDirectories(base.resolve("installed")).toString());
    }

    static void configureSession(
        RunContext runContext,
        Connection connection,
        String workingDirectory,
        Property<List<String>> communityExtensions
    ) throws Exception {
        if (workingDirectory != null) {
            execute(connection, "SET file_search_path='" + escapeSqlLiteral(workingDirectory) + "';");
        }

        for (String extension : renderExtensions(runContext, communityExtensions)) {
            if (!EXTENSION_NAME.matcher(extension).matches()) {
                runContext.logger().warn(
                    "Skipping DuckDB community extension '{}' because the name contains unsupported characters.",
                    extension
                );
                continue;
            }

            try {
                if (!tryLoadBundledExtension(runContext, connection, extension)) {
                    // The connection properties keep this fallback's cache in the task directory too.
                    // Installed/loaded extensions can be inspected with:
                    //   SELECT extension_name, installed, loaded, install_path FROM duckdb_extensions();
                    execute(connection, "INSTALL " + extension + " FROM community;");
                    execute(connection, "LOAD " + extension + ";");
                }
            } catch (SQLException e) {
                logExtensionWarning(runContext.logger(), extension, e);
            }
        }
    }

    private static List<String> renderExtensions(RunContext runContext, Property<List<String>> communityExtensions)
        throws IllegalVariableEvaluationException {
        if (communityExtensions == null) {
            return DuckDbQueryInterface.DEFAULT_COMMUNITY_EXTENSIONS;
        }

        List<String> rendered = runContext.render(communityExtensions).asList(String.class);
        return rendered == null ? List.of() : rendered;
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static boolean tryLoadBundledExtension(
        RunContext runContext,
        Connection connection,
        String extension
    ) {
        String platform = bundledPlatform();
        if (platform == null) {
            return false;
        }

        try {
            if (!loadBundledExtension(connection, runContext.workingDir().path(), extension, platform)) {
                runContext.logger().debug(
                    "Bundled DuckDB extension '{}' is not available for platform '{}'; falling back to community install.",
                    extension,
                    platform
                );
                return false;
            }

            runContext.logger().debug(
                "Loaded DuckDB extension '{}' from bundled {} binary.",
                extension,
                platform
            );
            return true;
        } catch (IOException | SQLException e) {
            runContext.logger().warn(
                "Failed to load bundled DuckDB extension '{}'. Falling back to community install. message={}",
                extension,
                e.getMessage()
            );
            return false;
        }
    }

    // Kept separate from the best-effort online fallback so tests can prove the
    // bundled path works by itself, with signature checks and networking disabled.
    static boolean loadBundledExtension(Connection connection, Path workingDirectory, String extension, String platform)
        throws IOException, SQLException {
        if (!EXTENSION_NAME.matcher(extension).matches()) {
            throw new IllegalArgumentException("Invalid DuckDB extension name: " + extension);
        }
        try (InputStream input = DuckDbConnectionSetup.class.getClassLoader().getResourceAsStream(bundledResourcePath(extension, platform))) {
            if (input == null) {
                return false;
            }
            Path repository = workingDirectory.resolve(".duckdb_extensions").resolve("bundled");
            Path extensionDirectory = repository.resolve(bundledDuckDbVersion()).resolve(platform);
            Files.createDirectories(extensionDirectory);
            Files.copy(input, extensionDirectory.resolve(extension + ".duckdb_extension"), java.nio.file.StandardCopyOption.REPLACE_EXISTING);

            String publicKeys = String.join(", ", bundledPublicKeys().stream()
                .map(key -> "'" + escapeSqlLiteral(key) + "'").toList());
            // Only allow extension repositories for the bundled install, then lock them with 'forbidden'
            // (DuckDB cannot go back to 'undecided') so the flow author's SQL cannot register their own repository and keys.
            execute(connection, "SET allow_extension_repositories = 'allowed';");
            try {
                execute(connection, "CREATE OR REPLACE EXTENSION REPOSITORY " + BUNDLED_REPOSITORY +
                    " WITH PREFIX '" + escapeSqlLiteral(repository.toAbsolutePath().toString()) +
                    "' USING PUBLIC KEYS " + publicKeys + ";");
                execute(connection, "INSTALL AND LOAD " + extension + " FROM " + BUNDLED_REPOSITORY + ";");
            } finally {
                execute(connection, "SET allow_extension_repositories = 'forbidden';");
            }
            return true;
        }
    }

    static List<String> bundledPublicKeys() throws IOException {
        try (InputStream input = DuckDbConnectionSetup.class.getClassLoader().getResourceAsStream(BUNDLED_PUBLIC_KEYS)) {
            if (input == null) {
                throw new IOException("Missing bundled DuckDB community public keys");
            }
            String pem = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            var matcher = Pattern.compile("-----BEGIN PUBLIC KEY-----.*?-----END PUBLIC KEY-----", Pattern.DOTALL).matcher(pem);
            List<String> keys = new ArrayList<>();
            while (matcher.find()) {
                keys.add(matcher.group());
            }
            if (keys.isEmpty()) {
                throw new IOException("No bundled DuckDB community public keys found");
            }
            return keys;
        }
    }

    static String bundledResourcePath(String extension, String platform) {
        return BUNDLED_EXTENSIONS_ROOT + "/" + bundledDuckDbVersion() + "/" + platform + "/" + extension + ".duckdb_extension";
    }

    static String bundledDuckDbVersion() {
        return BUNDLED_EXTENSION_PROPERTIES.getProperty("duckdb.version", "");
    }

    private static Properties loadBundledExtensionProperties() {
        Properties properties = new Properties();

        try (InputStream inputStream = DuckDbConnectionSetup.class.getClassLoader().getResourceAsStream(BUNDLED_EXTENSIONS_METADATA)) {
            if (inputStream != null) {
                properties.load(inputStream);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Unable to load DuckDB bundled extension metadata", e);
        }

        return properties;
    }

    static String bundledPlatform() {
        String osName = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (!osName.contains("linux")) {
            return null;
        }

        String osArch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
        return switch (osArch) {
            case "amd64", "x86_64" -> "linux_amd64";
            case "aarch64", "arm64" -> "linux_arm64";
            default -> null;
        };
    }

    private static void logExtensionWarning(Logger logger, String extension, SQLException e) {
        logger.warn(
            "Failed to install/load DuckDB community extension '{}'. Continuing without it. SQLState={}, message={}",
            extension,
            e.getSQLState(),
            e.getMessage()
        );
    }

    private static String escapeSqlLiteral(String value) {
        return value.replace("'", "''");
    }
}
