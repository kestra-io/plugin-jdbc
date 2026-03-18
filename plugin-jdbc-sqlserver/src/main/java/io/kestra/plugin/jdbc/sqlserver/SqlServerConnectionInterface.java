package io.kestra.plugin.jdbc.sqlserver;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.plugin.jdbc.JdbcConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;

public interface SqlServerConnectionInterface extends JdbcConnectionInterface {
    @Schema(
        title = "Whether to encrypt the connection.",
        description = """
            Controls JDBC encryption between the client and SQL Server.
            Defaults to FALSE to preserve backward compatibility with mssql-jdbc 12.x behavior.
            Set to TRUE or STRICT for encrypted connections.
            Note: if your SQL Server has "Force Encryption" enabled server-side, the server will \
            require TLS regardless of this setting. In that case, ensure your server supports TLS 1.2+ \
            and its certificate is properly configured, otherwise you may get "unexpected_message" errors \
            during the TLS handshake."""
    )
    @PluginProperty(group = "connection")
    Property<EncryptMode> getEncrypt();

    @Schema(
        title = "Whether to trust the server certificate without validation.",
        description = """
            When set to true, the driver does not validate the SQL Server TLS/SSL certificate.
            Useful for development or self-signed certificates."""
    )
    @PluginProperty(group = "connection")
    Property<Boolean> getTrustServerCertificate();

    @Schema(
        title = "The hostname expected in the server certificate.",
        description = """
            Specifies the host name to be used when validating the SQL Server TLS/SSL certificate.
            If not set, the driver uses the server name from the connection URL."""
    )
    @PluginProperty(group = "connection")
    Property<String> getHostNameInCertificate();

    @Schema(
        title = "The path to the trust store file.",
        description = """
            Specifies the path (including file name) to the certificate trust store file.
            Used when encrypt is TRUE or STRICT to validate the server certificate."""
    )
    @PluginProperty(group = "connection")
    Property<String> getTrustStore();

    @Schema(
        title = "The trust store password.",
        description = "The password used to access the trust store file."
    )
    @PluginProperty(group = "connection")
    Property<String> getTrustStorePassword();

    enum EncryptMode {
        TRUE,
        FALSE,
        STRICT,
        OPTIONAL
    }

    @Override
    default String getScheme() {
        return "jdbc:sqlserver";
    }
}
