package io.kestra.plugin.jdbc.access;

import io.kestra.core.runners.RunContext;

import java.nio.file.Path;
import java.util.Properties;

public class AccessQueryUtils {
    /**
     * Resolves the Access database path against the working directory and rewrites the JDBC URL.
     * When the file does not yet exist, appends newDatabaseVersion so UCanAccess creates it.
     */
    protected static Properties buildAccessProperties(Properties properties, RunContext runContext, AccessVersion newDatabaseVersion) {
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

        // memory=false uses a file-backed HSQLDB mirror instead of the default in-memory
        // catalog. Each task already receives a unique absolute path (UUID-based filename)
        // so the HSQLDB files land in isolated per-task working directories. This prevents
        // UCanAccess's in-memory DBReferenceSingleton from mixing catalogs across concurrent
        // task executions that share the same JVM.
        String params = ";memory=false";
        if (resolved.toFile().exists()) {
            properties.put("jdbc.url", "jdbc:ucanaccess://" + absolutePath + params);
        } else {
            // Auto-create the Access file when it does not exist yet.
            // newDatabaseVersion is a UCanAccess connection parameter documented at:
            // https://github.com/spannm/ucanaccess/wiki/connection-parameters
            properties.put("jdbc.url", "jdbc:ucanaccess://" + absolutePath + params + ";newDatabaseVersion=" + newDatabaseVersion.name());
        }

        return properties;
    }
}
