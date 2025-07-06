package io.kestra.plugin.jdbc;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.micronaut.http.uri.UriBuilder;
import io.swagger.v3.oas.annotations.media.Schema;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import jakarta.validation.constraints.NotNull;
import org.apache.commons.lang3.StringUtils;


public interface JdbcConnectionInterface {
    @Schema(
        title = "The JDBC URL to connect to the database."
    )
    @NotNull
    @PluginProperty(group = "connection")
    Property<String> getUrl();

    @Schema(
        title = "The database user."
    )
    @PluginProperty(group = "connection")
    Property<String> getUsername();

    @Schema(
        title = "The database user's password."
    )
    @PluginProperty(group = "connection")
    Property<String> getPassword();

    /**
     * JDBC driver may be auto-registered. See <a href="https://docs.oracle.com/javase/8/docs/api/java/sql/DriverManager.html">DriverManager</a>
     *
     * @throws SQLException registerDrivers failed
     */
    void registerDriver() throws SQLException;

    /**
     * Validation of the URL based on the database.
     * Default behavior is to render and check if the URL is empty and if it is a valid URL.
     * @return the validated and rendered URL
     * @throws IllegalVariableEvaluationException if there is a rendering issue.
     * @throws IllegalArgumentException if the URL is not valid or empty.
     */
    default String validateUrl(RunContext runContext) throws IllegalVariableEvaluationException, IllegalArgumentException {
        
        String url = runContext.render(this.getUrl()).as(String.class).orElse(null);

        //Check if URL is empty
        if (StringUtils.isBlank(url)) {
            throw new IllegalArgumentException("Rendered value of `url` is blank.");
        }

        //Validate JDBC scheme
        if (getScheme() != null && !url.startsWith(getScheme())) {
            throw new IllegalArgumentException("URL scheme is invalid. URL should start with `" + getScheme() + "`");
        }

        return url;
    }

    /***
     * Return the scheme associated to the JDBC driver for validation
     * @return jdbc scheme (ex: `jdbc:postgresql`)
     */
    String getScheme();

    default Properties connectionProperties(RunContext runContext) throws Exception {
        return createConnectionProperties(runContext);
    }

    default Properties connectionProperties(RunContext runContext, String urlScheme) throws Exception {
        Properties props = createConnectionProperties(runContext);

        URI url = URI.create((String) props.get("jdbc.url"));
        url = URI.create(url.getSchemeSpecificPart());

        UriBuilder builder = UriBuilder.of(url).scheme(urlScheme);

        props.put("jdbc.url", builder.build().toString());

        return props;
    }

    default Connection connection(RunContext runContext) throws Exception {
        registerDriver();

        Properties props = this.connectionProperties(runContext);
        String jdbcUrl = props.getProperty("jdbc.url");
        props.remove("jdbc.url");

        return DriverManager.getConnection(jdbcUrl, props);
    }

    private Properties createConnectionProperties(RunContext runContext) throws IllegalVariableEvaluationException {
        Properties props = new Properties();
        props.put("jdbc.url", validateUrl(runContext));

        if (this.getUsername() != null) {
            props.put("user", runContext.render(this.getUsername()).as(String.class).orElse(null));
        }

        if (this.getPassword() != null) {
            props.put("password", runContext.render(this.getPassword()).as(String.class).orElse(null));
        }

        return props;
    }
}
