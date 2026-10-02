package io.kestra.plugin.jdbc.mysql;

import io.kestra.plugin.jdbc.JdbcConnectionInterface;
import io.micronaut.http.uri.UriBuilder;

import java.net.URI;
import java.nio.file.Path;
import java.util.Properties;

public interface MySqlConnectionInterface extends JdbcConnectionInterface {
    @Override
    default String getScheme() {
        return "jdbc:mysql";
    }

    // loadLocalInfilePath is null when no inputFile, since a per-run path in the URL would defeat pooling (the URL is part of the pool key).
    default Properties createMysqlProperties(Properties props, Path loadLocalInfilePath,
                                             boolean isMultiQuery) {
        URI url = URI.create((String) props.get("jdbc.url"));
        url = URI.create(url.getSchemeSpecificPart());

        UriBuilder builder = UriBuilder.of(url);

        // allow local in file for current worker and prevent the global one
        if (loadLocalInfilePath != null) {
            builder.queryParam("allowLoadLocalInfileInPath", loadLocalInfilePath.toAbsolutePath().toString());
        }
        builder.replaceQueryParam("allowLoadLocalInfile", false);

        // see https://dev.mysql.com/doc/connector-j/en/connector-j-reference-implementation-notes.html
        // By default, ResultSets are completely retrieved and stored in memory.
        builder.replaceQueryParam("useCursorFetch", true);

        builder.scheme("jdbc:mysql");

        if (isMultiQuery) {
            builder.queryParam("allowMultiQueries", true);
        }

        props.put("jdbc.url", builder.build().toString());

        return props;
    }
}
