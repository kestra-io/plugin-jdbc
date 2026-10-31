package io.kestra.plugin.jdbc.trino;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * How the Trino JDBC driver validates the TLS certificate of the coordinator.
 */
@Schema(description = "Names of the `SSLVerification` modes of the Trino JDBC driver")
public enum SslVerification {
    /** Validate the server certificate chain against the JVM truststore (driver default). */
    FULL,
    /** Validate the server certificate chain against the configured truststore only. */
    CA,
    /** Skip certificate validation (for example a self-signed development coordinator). */
    NONE
}
