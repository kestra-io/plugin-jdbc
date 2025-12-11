package io.kestra.plugin.jdbc.mariadb;

import io.micronaut.http.uri.UriBuilder;

import java.net.URI;
import java.nio.file.Path;
import java.util.Properties;

public abstract class MariaDbUtils {
    protected static Properties createMariaDbProperties(Properties props, boolean isMultiQuery) {
        URI url = URI.create((String) props.get("jdbc.url"));
        url = URI.create(url.getSchemeSpecificPart());

        UriBuilder builder = UriBuilder.of(url);

        // By default, ResultSets are completely retrieved and stored in memory.
        builder.replaceQueryParam("useCursorFetch", true);

        builder.scheme("jdbc:mariadb");

        if(isMultiQuery) {
            builder.queryParam("allowMultiQueries", true);
        }

        props.put("jdbc.url", builder.build().toString());

        return props;
    }
}
