package io.kestra.plugin.jdbc;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import jakarta.validation.constraints.NotNull;


public interface JdbcConnectionInterface {
    @Schema(
        title = "The JDBC URL to connect to the database."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getUrl();

    @Schema(
        title = "The database user."
    )
    @PluginProperty(dynamic = true)
    String getUsername();

    @Schema(
        title = "The database user's password."
    )
    @PluginProperty(dynamic = true)
    String getPassword();

    /**
     * JDBC driver may be auto-registered. See <a href="https://docs.oracle.com/javase/8/docs/api/java/sql/DriverManager.html">DriverManager</a>
     *
     * @throws SQLException registerDrivers failed
     */
    void registerDriver() throws SQLException;

    default Properties connectionProperties(RunContext runContext) throws Exception {
        Properties props = new Properties();
        props.put("jdbc.url", runContext.render(this.getUrl()));

        if (this.getUsername() != null) {
            props.put("user", runContext.render(this.getUsername()));
        }

        if (this.getPassword() != null) {
            props.put("password", runContext.render(this.getPassword()));
        }

        return props;
    }

    default Connection connection(RunContext runContext) throws Exception {
        registerDriver();

        Properties props = this.connectionProperties(runContext);
        String jdbcUrl = props.getProperty("jdbc.url");
        props.remove("jdbc.url");

        return DriverManager.getConnection(jdbcUrl, props);
    }
}
