package io.kestra.plugin.jdbc.trino;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.JdbcConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Typed connection properties for the Trino JDBC driver.
 *
 * <p>
 * Properties are rendered into the driver {@link Properties} by {@link #renderProperties}, so
 * authentication alternatives to {@code user}/{@code password}, session tuning and client
 * identification can be declared as task properties instead of being packed into the JDBC URL.
 *
 * <p>
 * Property keys are the ones the driver expects (see {@code io.trino.client.uri.PropertyName}).
 */
public interface TrinoConnectionInterface extends JdbcConnectionInterface {

    @Override
    default String getScheme() {
        return "jdbc:trino";
    }

    @Override
    default Properties connectionProperties(RunContext runContext) throws Exception {
        Properties properties = JdbcConnectionInterface.super.connectionProperties(runContext);

        renderProperties(runContext, properties);

        return properties;
    }

    @PluginProperty(group = "connection", secret = true)
    @Schema(
        title = "The Trino access token used for JWT-based authentication",
        description = """
            Sent as the `accessToken` connection property of the Trino JDBC driver, typically \
            through `{{ secret('TRINO_ACCESS_TOKEN') }}` for an engine behind an OAuth2/OIDC authentication provider.
            Browser-based OAuth2 (`externalAuthentication`) is not supported as it cannot complete on a headless worker."""
    )
    Property<String> getAccessToken();

    @PluginProperty(group = "connection")
    @Schema(
        title = "The default catalog the connection runs against",
        description = "Alternative to including the catalog in the JDBC URL path (`jdbc:trino://host:port/<catalog>`)."
    )
    Property<String> getCatalog();

    @PluginProperty(group = "connection")
    @Schema(
        title = "The default schema the connection runs against",
        description = "Only used when `catalog` is set, either here or in the JDBC URL path."
    )
    Property<String> getSchema();

    @PluginProperty(group = "connection")
    @Schema(
        title = "Whether to use TLS/SSL when connecting to Trino",
        description = """
            Sets the `SSL` connection property of the Trino JDBC driver.
            Required with a `password` (the driver refuses authentication with a password over plain HTTP) \
            and when the coordinator certificate must be validated by `sslTrustStore`.
            This is the only property the driver accepts both from the URL and here; when set it overrides the URL value."""
    )
    Property<Boolean> getSsl();

    @PluginProperty(group = "connection")
    @Schema(
        title = "The TLS/SSL certificate verification policy used by the Trino JDBC driver",
        description = """
            `FULL` (driver default) validates the server certificate chain against the JVM truststore, \
            `CA` validates it against the configured `sslTrustStore` only, \
            and `NONE` skips validation (for example, a self-signed development coordinator)."""
    )
    Property<SslVerification> getSslVerification();

    @PluginProperty(group = "connection", secret = true)
    @Schema(
        title = "The client keystore content used for mutual TLS (client certificate) authentication",
        description = """
            The keystore *content* — the bytes of a PKCS12 or JKS keystore base64-encoded, or a `kestra://` internal storage file URI — \
            written to the task working directory and passed to the driver as `SSLKeyStorePath`, so nothing has to be \
            pre-mounted on the worker. When this is set, `ssl: true` is expected as well."""
    )
    Property<String> getSslKeyStore();

    @PluginProperty(group = "connection", secret = true)
    @Schema(
        title = "The password of the client keystore, when it is protected"
    )
    Property<String> getSslKeyStorePassword();

    @PluginProperty(group = "connection")
    @Schema(
        title = "The type of the client keystore, `PKCS12` (default) or `JKS`",
        description = "Passed to the driver as `SSLKeyStoreType`."
    )
    Property<String> getSslKeyStoreType();

    @PluginProperty(group = "connection")
    @Schema(
        title = "The truststore content used to validate the Trino coordinator certificate chain",
        description = """
            The truststore *content* — the bytes of a PKCS12 or JKS truststore base64-encoded, or a `kestra://` internal \
            storage file URI — written to the task working directory and passed to the driver as `SSLTrustStorePath`."""
    )
    Property<String> getSslTrustStore();

    @PluginProperty(group = "connection", secret = true)
    @Schema(
        title = "The password of the truststore, when it is protected"
    )
    Property<String> getSslTrustStorePassword();

    @PluginProperty(group = "connection")
    @Schema(
        title = "The type of the truststore, `PKCS12` (default) or `JKS`",
        description = "Passed to the driver as `SSLTrustStoreType`."
    )
    Property<String> getSslTrustStoreType();

    @PluginProperty(group = "connection")
    @Schema(
        title = "The Kerberos principal the connection authenticates as",
        description = "Passed to the driver as `KerberosPrincipal`."
    )
    Property<String> getKerberosPrincipal();

    @PluginProperty(group = "connection")
    @Schema(
        title = "The remote service (cluster coordinator) name Kerberos tickets are requested for",
        description = "Passed to the driver as `KerberosRemoteServiceName`."
    )
    Property<String> getKerberosRemoteServiceName();

    @PluginProperty(group = "connection", secret = true)
    @Schema(
        title = "The keytab file content holding the user Kerberos keys",
        description = "Written to the task working directory and passed to the driver as `KerberosKeytabPath`, so the keytab does not have to be pre-mounted on the worker.")
    Property<String> getKerberosKeytab();

    @PluginProperty(group = "connection")
    @Schema(
        title = "The Kerberos configuration (krb5.conf) file content",
        description = "Written to the task working directory and passed to the driver as `KerberosConfigPath`.")
    Property<String> getKerberosConfig();

    @PluginProperty(group = "advanced")
    @Schema(
        title = "The Trino session properties to set at connection time",
        description = """
            Catalog-scoped properties use the `catalog.property` Java-style name (e.g. `hive.parquet_use_column_names`). \
            Passed to the driver as the `sessionProperties` connection property (`name:value` pairs joined with `;`)."""
    )
    Property<Map<String, Object>> getSessionProperties();

    @PluginProperty(group = "advanced")
    @Schema(
        title = "The tags attached to the queries run by this task",
        description = """
            Lets Trino administrators route Kestra queries in resource groups managed by `resourceGroupManager`. \
            Passed to the driver as the `clientTags` connection property (comma-separated tag names)."""
    )
    Property<List<String>> getClientTags();

    @PluginProperty(group = "advanced")
    @Schema(
        title = "The client source name reported to the coordinator for every query of this task",
        description = """
            Visible in `system.runtime.queries` and in the coordinator UI and usable in `resourceGroupManager` selectors: \
            defaults to `kestra` so Kestra traffic can be identified and capacity-planned independently of other Trino clients."""
    )
    Property<String> getSource();

    @PluginProperty(group = "advanced")
    @Schema(
        title = "The catalog roles to use, as a catalog name to role mapping",
        description = """
            Role values are a named role (e.g. `admin`), `all` or `none`, and the pseudo-catalog `system` sets the role \
            of every catalog without an explicit entry. Passed to the driver as the `roles` connection property \
            (`catalog:role` values joined with `;`)."""
    )
    Property<Map<String, Object>> getRoles();

    @PluginProperty(group = "advanced", secret = true)
    @Schema(
        title = "Extra authentication or connector-specific credentials sent as `extraCredentials`",
        description = """
            Name:value pairs forwarded to the coordinator as authentication extra credentials (e.g. Kerberos delegation tokens, \
            cloud object store keys for catalog authentication). The driver serializes them as `name:value` pairs joined with `;`."""
    )
    Property<Map<String, Object>> getExtraCredentials();

    /**
     * Renders the typed connection properties into the driver connection properties.
     */
    private void renderProperties(RunContext runContext, Properties properties) throws Exception {
        String url = properties.getProperty("jdbc.url");

        putString(runContext, properties, "accessToken", this.getAccessToken());
        putString(runContext, properties, "catalog", this.getCatalog());
        putString(runContext, properties, "schema", this.getSchema());

        var rSsl = runContext.render(this.getSsl()).as(Boolean.class).orElse(null);
        if (rSsl != null) {
            properties.setProperty("SSL", rSsl.toString());

        }

        var rSslVerification = runContext.render(this.getSslVerification()).as(SslVerification.class).orElse(null);
        if (rSslVerification != null) {
            properties.setProperty("SSLVerification", rSslVerification.name());

        }

        materializeKeystore(
            runContext, properties,
            "keystore", this.getSslKeyStore(), this.getSslKeyStorePassword(), this.getSslKeyStoreType(),
            "SSLKeyStorePath", "SSLKeyStorePassword", "SSLKeyStoreType"
        );
        materializeKeystore(
            runContext, properties,
            "truststore", this.getSslTrustStore(), this.getSslTrustStorePassword(), this.getSslTrustStoreType(),
            "SSLTrustStorePath", "SSLTrustStorePassword", "SSLTrustStoreType"
        );

        putString(runContext, properties, "KerberosPrincipal", this.getKerberosPrincipal());
        putString(runContext, properties, "KerberosRemoteServiceName", this.getKerberosRemoteServiceName());

        // The driver only accepts file paths for the keytab and the Kerberos configuration, so their content is
        // materialized on the worker and the path is passed instead.
        materialContent(runContext, properties, "keytab", this.getKerberosKeytab(), "KerberosKeytabPath");
        materialContent(runContext, properties, "krb5.conf", this.getKerberosConfig(), "KerberosConfigPath");

        putMap(runContext, properties, "sessionProperties", this.getSessionProperties());
        putList(runContext, properties, "clientTags", this.getClientTags());
        putString(runContext, properties, "source", this.getSource());
        if (this.getSource() == null && !urlPropertyKeys(url).contains("source")) {
            properties.setProperty("source", "kestra");
        }
        putMap(runContext, properties, "roles", this.getRoles());
        putMap(runContext, properties, "extraCredentials", this.getExtraCredentials());

    }

    /**
     * The property keys carried by the JDBC URL itself: its query parameters plus the catalog and schema
     * of the URL path, which the Trino JDBC driver counts as properties before merging the task ones.
     */
    private static Set<String> urlPropertyKeys(String url) {
        var keys = new TreeSet<String>();
        if (url == null || url.isEmpty()) {
            return keys;
        }

        try {
            var serverUri = URI.create(url.substring("jdbc:trino:".length()));
            var rawQuery = serverUri.getRawQuery();
            if (rawQuery != null) {
                for (var pair : rawQuery.split("&")) {
                    if (pair.isBlank()) {
                        continue;
                    }
                    var name = pair.contains("=") ? pair.substring(0, pair.indexOf('=')) : pair;
                    keys.add(URLDecoder.decode(name, StandardCharsets.UTF_8));
                }
            }

            var path = serverUri.getPath();
            if (path != null) {
                // `[schema]` is `"", "catalog"[, "schema"]` because of the leading `/`
                var segments = path.split("/");
                if (segments.length >= 2 && !segments[1].isBlank()) {
                    keys.add("catalog");
                }
                if (segments.length >= 3 && !segments[2].isBlank()) {
                    keys.add("schema");
                }
            }
        } catch (RuntimeException unused) {
            // Best effort: the check must not break valid URLs, and the driver validates the final URL anyway.
        }
        return keys;
    }

    private static void putString(RunContext runContext, Properties properties, String key, Property<String> property) throws Exception {
        var value = runContext.render(property).as(String.class).orElse(null);
        if (value != null && !value.isEmpty()) {
            properties.setProperty(key, value);

        }
    }

    private static void putMap(RunContext runContext, Properties properties, String key, Property<Map<String, Object>> property) throws Exception {
        var values = runContext.render(property).asMap(String.class, Object.class);
        if (!values.isEmpty()) {
            properties.setProperty(key, values.entrySet().stream()
                .map(entry -> entry.getKey() + ":" + entry.getValue())
                .collect(Collectors.joining(";")));

        }
    }

    private static void putList(RunContext runContext, Properties properties, String key, Property<List<String>> property) throws Exception {
        var values = runContext.render(property).asList(String.class);
        if (!values.isEmpty()) {
            properties.setProperty(key, String.join(",", values));

        }
    }

    /**
     * Writes a keystore (client certificate or truststore) content in the task working directory and passes its
     * path, plus the optional password and type, to the driver.
     */
    private static void materializeKeystore(
        RunContext runContext, Properties properties,
        String filename, Property<String> content, Property<String> password, Property<String> type,
        String pathKey, String passwordKey, String typeKey
    ) throws Exception {
        var rContent = runContext.render(content).as(String.class).orElse(null);
        if (rContent == null || rContent.isBlank()) {
            return;
        }

        properties.setProperty(pathKey, materializeFile(runContext, filename, contentBytes(runContext, rContent)));


        putString(runContext, properties, passwordKey, password);

        var rType = runContext.render(type).as(String.class).orElse(null);
        if (rType != null && !rType.isBlank()) {
            properties.setProperty(typeKey, rType);

        }
    }

    /** Writes a plain file content in the task working directory and passes its path to the driver. */
    private static void materialContent(RunContext runContext, Properties properties, String filename, Property<String> property, String pathKey) throws Exception {
        var rContent = runContext.render(property).as(String.class).orElse(null);
        if (rContent == null || rContent.isBlank()) {
            return;
        }

        properties.setProperty(pathKey, materializeFile(runContext, filename, contentBytes(runContext, rContent)));

    }

    /**
     * The bytes of a keystore, truststore or credentials file content: `kestra://` internal storage URIs are read
     * from storage, strict base64 strings decode to their raw bytes, and anything else is kept as UTF-8 text
     * (e.g. a PEM bundle).
     */
    private static byte[] contentBytes(RunContext runContext, String content) throws IOException {
        if (content.startsWith("kestra://")) {
            try (InputStream input = runContext.storage().getFile(URI.create(content))) {
                return input.readAllBytes();
            }
        }

        try {
            return Base64.getDecoder().decode(content);
        } catch (IllegalArgumentException e) {
            return content.getBytes(StandardCharsets.UTF_8);
        }
    }

    /**
     * Writes the content in the task working directory, so the file shows up in the task UI.
     */
    private static String materializeFile(RunContext runContext, String filename, byte[] bytes) throws IOException {
        Path file = runContext.workingDir().createTempFile("." + filename);
        Files.write(file, bytes);
        return file.toAbsolutePath().toString();
    }
}
