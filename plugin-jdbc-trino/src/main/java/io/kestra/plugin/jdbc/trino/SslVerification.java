package io.kestra.plugin.jdbc.trino;


/**
 * How the Trino JDBC driver validates the TLS certificate of the coordinator.
 */
public enum SslVerification {
    /** Validate the server certificate chain against the JVM truststore (driver default). */
    FULL,
    /** Validate the server certificate chain against the configured truststore only. */
    CA,
    /** Skip certificate validation (for example a self-signed development coordinator). */
    NONE
}
