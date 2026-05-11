package io.kestra.plugin.jdbc.access;

import io.kestra.core.runners.RunContext;

import java.nio.file.Path;
import java.util.Properties;

public class AccessQueryUtils {
    /**
     * Resolves the Access database path against the working directory and rewrites the JDBC URL.
     * When the file does not yet exist, appends newDatabaseVersion=V2010 so UCanAccess creates it.
     */
    protected static Properties buildAccessProperties(Properties properties, RunContext runContext) {
        var url = (String) properties.get("jdbc.url");

        if (url == null || !url.startsWith("jdbc:ucanaccess://")) {
            return properties;
        }

        // Strip any existing query parameters before path extraction
        var urlNoParams = url.contains(";") ? url.substring(0, url.indexOf(';')) : url;
        // After "jdbc:ucanaccess://" we have the file path (may be absolute or relative)
        var rawPath = urlNoParams.substring("jdbc:ucanaccess://".length());

        Path filePath = Path.of(rawPath);
        // For relative paths, resolve against the working directory
        Path resolved = filePath.isAbsolute()
            ? filePath
            : runContext.workingDir().resolve(filePath);

        var absolutePath = resolved.toAbsolutePath().toString();

        if (resolved.toFile().exists()) {
            properties.put("jdbc.url", "jdbc:ucanaccess://" + absolutePath);
        } else {
            // Auto-create the Access file when it does not exist yet
            properties.put("jdbc.url", "jdbc:ucanaccess://" + absolutePath + ";newDatabaseVersion=V2010");
        }

        return properties;
    }
}
