package io.kestra.plugin.jdbc.postgresql;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.plugin.jdbc.JdbcConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;

public interface PostgresConnectionInterface extends JdbcConnectionInterface {
    @Schema(
        title = "Is the connection ssl"
    )
    @PluginProperty(dynamic = false)
    Boolean getSsl();

    @Schema(
        title = "The ssl mode"
    )
    @PluginProperty(dynamic = false)
    SslMode getSslMode();

    @Schema(
        title = "The ssl root cert",
        description = "Must be a PEM encoded certificate"
    )
    @PluginProperty(dynamic = true)
    String getSslRootCert();

    @Schema(
        title = "The ssl cert",
        description = "Must be a PEM encoded certificate"
    )
    @PluginProperty(dynamic = true)
    String getSslCert();

    @Schema(
        title = "The ssl key",
        description = "Must be a PEM encoded key"
    )
    @PluginProperty(dynamic = true)
    String getSslKey();

    @Schema(
        title = "The ssl key password"
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
