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

    default Properties createMysqlProperties(Properties props, Path workingDirectory,
                                                      boolean isMultiQuery) {
        URI url = URI.create((String) props.get("jdbc.url"));
        url = URI.create(url.getSchemeSpecificPart());

        UriBuilder builder = UriBuilder.of(url);

        // allow local in file for current worker and prevent the global one
        builder.queryParam("allowLoadLocalInfileInPath", workingDirectory.toAbsolutePath().toString());
        builder.replaceQueryParam("allowLoadLocalInfile", false);

        // see https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-implementation-notes.html
        // By default, ResultSets are completely retrieved and stored in memory.
        builder.replaceQueryParam("useCursorFetch", true);

        builder.scheme("jdbc:mysql");

        if(isMultiQuery) {
            builder.queryParam("allowMultiQueries", true);
        }

        props.put("jdbc.url", builder.build().toString());

        return props;
    }
}
