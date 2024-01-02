package io.kestra.plugin.jdbc.postgresql;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.plugin.jdbc.JdbcConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;

public interface PostgresConnectionInterface extends JdbcConnectionInterface {
    @Schema(
        title = "Is the connection SSL?"
    )
    @PluginProperty(dynamic = false)
    Boolean getSsl();

    @Schema(
        title = "The SSL mode."
    )
    @PluginProperty(dynamic = false)
    SslMode getSslMode();

    @Schema(
        title = "The SSL root cert.",
        description = "Must be a PEM encoded certificate"
    )
    @PluginProperty(dynamic = true)
    String getSslRootCert();

    @Schema(
        title = "The SSL cert.",
        description = "Must be a PEM encoded certificate"
    )
    @PluginProperty(dynamic = true)
    String getSslCert();

    @Schema(
        title = "The SSL key.",
        description = "Must be a PEM encoded key"
    )
    @PluginProperty(dynamic = true)
    String getSslKey();

    @Schema(
        title = "The SSL key password."
    )
    @PluginProperty(dynamic = true)
    String getSslKeyPassword();

    enum SslMode {
        DISABLE,
        ALLOW,
        PREFER,
        REQUIRE,
        VERIFY_CA,
        VERIFY_FULL
    }
}
