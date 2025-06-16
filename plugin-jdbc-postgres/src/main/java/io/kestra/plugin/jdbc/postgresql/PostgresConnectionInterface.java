package io.kestra.plugin.jdbc.postgresql;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.plugin.jdbc.JdbcConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;

public interface PostgresConnectionInterface extends JdbcConnectionInterface {
    @Schema(
        title = "Is the connection SSL?"
    )
    @PluginProperty(group = "connection")
    Property<Boolean> getSsl();

    @Schema(
        title = "The SSL mode."
    )
    @PluginProperty(group = "connection")
    Property<SslMode> getSslMode();

    @Schema(
        title = "The SSL root cert.",
        description = "Must be a PEM encoded certificate"
    )
    @PluginProperty(group = "connection")
    Property<String> getSslRootCert();

    @Schema(
        title = "The SSL cert.",
        description = "Must be a PEM encoded certificate"
    )
    @PluginProperty(group = "connection")
    Property<String> getSslCert();

    @Schema(
        title = "The SSL key.",
        description = "Must be a PEM encoded key"
    )
    @PluginProperty(group = "connection")
    Property<String> getSslKey();

    @Schema(
        title = "The SSL key password."
    )
    @PluginProperty(group = "connection")
    Property<String> getSslKeyPassword();

    enum SslMode {
        DISABLE,
        ALLOW,
        PREFER,
        REQUIRE,
        VERIFY_CA,
        VERIFY_FULL
    }

    @Override
    default String getScheme() {
        return "jdbc:postgresql";
    }
}
