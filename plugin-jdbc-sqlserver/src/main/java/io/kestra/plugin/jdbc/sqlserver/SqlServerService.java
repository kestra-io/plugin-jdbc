package io.kestra.plugin.jdbc.sqlserver;

import io.kestra.core.runners.RunContext;

import java.util.Locale;
import java.util.Properties;

public abstract class SqlServerService {
    public static void handleSsl(Properties properties, RunContext runContext, SqlServerConnectionInterface conn) throws Exception {
        var jdbcUrl = properties.getProperty("jdbc.url");

        if (conn.getEncrypt() != null) {
            var rEncrypt = runContext.render(conn.getEncrypt()).as(SqlServerConnectionInterface.EncryptMode.class).orElse(null);
            putUnlessInUrl(properties, jdbcUrl, "encrypt", rEncrypt == null ? null : rEncrypt.name().toLowerCase(Locale.ROOT));
        }

        if (conn.getTrustServerCertificate() != null) {
            var rTrustServerCertificate = runContext.render(conn.getTrustServerCertificate()).as(Boolean.class).orElse(null);
            putUnlessInUrl(properties, jdbcUrl, "trustServerCertificate", rTrustServerCertificate == null ? null : rTrustServerCertificate.toString());
        }

        if (conn.getHostNameInCertificate() != null) {
            var rHostNameInCertificate = runContext.render(conn.getHostNameInCertificate()).as(String.class).orElse(null);
            putUnlessInUrl(properties, jdbcUrl, "hostNameInCertificate", rHostNameInCertificate);
        }

        if (conn.getTrustStore() != null) {
            var rTrustStore = runContext.render(conn.getTrustStore()).as(String.class).orElse(null);
            putUnlessInUrl(properties, jdbcUrl, "trustStore", rTrustStore);
        }

        if (conn.getTrustStorePassword() != null) {
            var rTrustStorePassword = runContext.render(conn.getTrustStorePassword()).as(String.class).orElse(null);
            putUnlessInUrl(properties, jdbcUrl, "trustStorePassword", rTrustStorePassword);
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
