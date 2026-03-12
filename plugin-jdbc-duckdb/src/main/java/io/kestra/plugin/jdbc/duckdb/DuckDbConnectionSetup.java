package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.regex.Pattern;

final class DuckDbConnectionSetup {
    private static final Pattern EXTENSION_NAME = Pattern.compile("[A-Za-z0-9_]+");

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
                // DuckDB caches installed community extensions under ~/.duckdb/extensions/<version>/<platform> by default.
                // Deployments can override that cache location with:
                //   SET extension_directory = '/path/to/duckdb/extensions';
                // Installed/loaded extensions can be inspected with:
                //   SELECT extension_name, installed, loaded, install_path FROM duckdb_extensions();
                execute(connection, "INSTALL " + extension + " FROM community;");
                execute(connection, "LOAD " + extension + ";");
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
