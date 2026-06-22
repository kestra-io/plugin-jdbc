package io.kestra.plugin.jdbc.sqlserver;

import io.kestra.core.runners.RunContext;

import java.util.Locale;
import java.util.Properties;

public abstract class SqlServerService {
    public static void handleSsl(Properties properties, RunContext runContext, SqlServerConnectionInterface conn) throws Exception {
        var jdbcUrl = properties.getProperty("jdbc.url");

        if (conn.getEncrypt() != null) {
            var encrypt = runContext.render(conn.getEncrypt()).as(SqlServerConnectionInterface.EncryptMode.class).orElse(null);
            putUnlessInUrl(properties, jdbcUrl, "encrypt", encrypt == null ? null : encrypt.name().toLowerCase(Locale.ROOT));
        }

        if (conn.getTrustServerCertificate() != null) {
            var trustServerCertificate = runContext.render(conn.getTrustServerCertificate()).as(Boolean.class).orElse(null);
            putUnlessInUrl(properties, jdbcUrl, "trustServerCertificate", trustServerCertificate == null ? null : trustServerCertificate.toString());
        }

        if (conn.getHostNameInCertificate() != null) {
            var hostNameInCertificate = runContext.render(conn.getHostNameInCertificate()).as(String.class).orElse(null);
            putUnlessInUrl(properties, jdbcUrl, "hostNameInCertificate", hostNameInCertificate);
        }

        if (conn.getTrustStore() != null) {
            var trustStore = runContext.render(conn.getTrustStore()).as(String.class).orElse(null);
            putUnlessInUrl(properties, jdbcUrl, "trustStore", trustStore);
        }

        if (conn.getTrustStorePassword() != null) {
            var trustStorePassword = runContext.render(conn.getTrustStorePassword()).as(String.class).orElse(null);
            putUnlessInUrl(properties, jdbcUrl, "trustStorePassword", trustStorePassword);
        }
    }

    private static void putUnlessInUrl(Properties properties, String jdbcUrl, String name, String value) {
        // mssql-jdbc gives the Properties object precedence over URL params, so a value already
        // present in the URL must not be overwritten here.
        if (value != null && !urlContainsParam(jdbcUrl, name)) {
            properties.put(name, value);
        }
    }

    private static boolean urlContainsParam(String jdbcUrl, String paramName) {
        if (jdbcUrl == null) {
            return false;
        }
        var lowerUrl = jdbcUrl.toLowerCase(Locale.ROOT);
        var lowerParam = paramName.toLowerCase(Locale.ROOT);
        return lowerUrl.contains(";" + lowerParam + "=") || lowerUrl.contains(";" + lowerParam + ";");
    }
}
