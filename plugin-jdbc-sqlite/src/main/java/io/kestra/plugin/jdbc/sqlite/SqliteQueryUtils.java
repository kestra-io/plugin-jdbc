package io.kestra.plugin.jdbc.sqlite;

import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.runners.RunContext;
import io.micronaut.http.uri.UriBuilder;

import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

public class SqliteQueryUtils {
    protected static Properties buildSqliteProperties(Properties properties, RunContext runContext) {
        URI url = URI.create((String) properties.get("jdbc.url"));

        // get file name from url scheme parts
        String filename = url.getSchemeSpecificPart().split(":")[1];

        Path path = runContext.workingDir().resolve(Path.of(filename));
        if (path.toFile().exists()) {
            url = URI.create(path.toString());
            UriBuilder builder = UriBuilder.of(url);
            builder.scheme("jdbc:sqlite");
            properties.put("jdbc.url", builder.build().toString());
        }

        return properties;
    }
}
