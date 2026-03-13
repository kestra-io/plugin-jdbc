package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Pattern;

final class DuckDbConnectionSetup {
    private static final Pattern EXTENSION_NAME = Pattern.compile("[A-Za-z0-9_]+");
    private static final String BUNDLED_EXTENSIONS_ROOT = "duckdb-community-extensions";
    private static final String BUNDLED_EXTENSIONS_METADATA = "duckdb-community-extensions.properties";
    private static final Properties BUNDLED_EXTENSION_PROPERTIES = loadBundledExtensionProperties();

    private DuckDbConnectionSetup() {
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
                if (!tryLoadBundledExtension(runContext, connection, workingDirectory, extension)) {
                    // DuckDB caches installed community extensions under ~/.duckdb/extensions/<version>/<platform> by default.
                    // Deployments can override that cache location with:
                    //   SET extension_directory = '/path/to/duckdb/extensions';
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
        String workingDirectory,
        String extension
    ) {
        String platform = bundledPlatform();
        if (platform == null) {
            return false;
        }

        String resourcePath = bundledResourcePath(extension, platform);
        try (InputStream inputStream = DuckDbConnectionSetup.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (inputStream == null) {
                runContext.logger().debug(
                    "Bundled DuckDB extension '{}' is not available for platform '{}'; falling back to community install.",
                    extension,
                    platform
                );
                return false;
            }

            Path extractedExtension = extractBundledExtension(inputStream, workingDirectory, extension, platform);
            execute(connection, "LOAD '" + escapeSqlLiteral(extractedExtension.toAbsolutePath().toString()) + "';");
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

    private static String bundledResourcePath(String extension, String platform) {
        return BUNDLED_EXTENSIONS_ROOT + "/" + bundledDuckDbVersion() + "/" + platform + "/" + extension + ".duckdb_extension";
    }

    private static String bundledDuckDbVersion() {
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

    private static Path extractBundledExtension(
        InputStream inputStream,
        String workingDirectory,
        String extension,
        String platform
    ) throws IOException {
        Path baseDir = workingDirectory == null
            ? Files.createTempDirectory("kestra-duckdb-extension-")
            : Path.of(workingDirectory).resolve(".duckdb_extensions").resolve(platform);
        Files.createDirectories(baseDir);

        Path extractedExtension = baseDir.resolve(extension + ".duckdb_extension");
        Files.copy(inputStream, extractedExtension, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        return extractedExtension;
    }

    private static String bundledPlatform() {
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
