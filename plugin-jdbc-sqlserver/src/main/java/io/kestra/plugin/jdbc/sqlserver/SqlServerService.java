package io.kestra.plugin.jdbc.sqlserver;

import io.kestra.core.runners.RunContext;

import java.util.Locale;
import java.util.Properties;

public abstract class SqlServerService {
    public static void handleSsl(Properties properties, RunContext runContext, SqlServerConnectionInterface conn) throws Exception {
        var jdbcUrl = properties.getProperty("jdbc.url");

        if (conn.getEncrypt() != null) {
            var rEncrypt = runContext.render(conn.getEncrypt()).as(SqlServerConnectionInterface.EncryptMode.class).orElse(null);
            if (rEncrypt != null && !urlContainsParam(jdbcUrl, "encrypt")) {
                properties.put("encrypt", rEncrypt.name().toLowerCase(Locale.ROOT));
            }
        }

        if (conn.getTrustServerCertificate() != null) {
            var rTrustServerCertificate = runContext.render(conn.getTrustServerCertificate()).as(Boolean.class).orElse(null);
            if (rTrustServerCertificate != null && !urlContainsParam(jdbcUrl, "trustServerCertificate")) {
                properties.put("trustServerCertificate", rTrustServerCertificate.toString());
            }
        }

        if (conn.getHostNameInCertificate() != null) {
            var rHostNameInCertificate = runContext.render(conn.getHostNameInCertificate()).as(String.class).orElse(null);
            if (rHostNameInCertificate != null && !urlContainsParam(jdbcUrl, "hostNameInCertificate")) {
                properties.put("hostNameInCertificate", rHostNameInCertificate);
            }
        }

        if (conn.getTrustStore() != null) {
            var rTrustStore = runContext.render(conn.getTrustStore()).as(String.class).orElse(null);
            if (rTrustStore != null && !urlContainsParam(jdbcUrl, "trustStore")) {
                properties.put("trustStore", rTrustStore);
            }
        }

        if (conn.getTrustStorePassword() != null) {
            var rTrustStorePassword = runContext.render(conn.getTrustStorePassword()).as(String.class).orElse(null);
            if (rTrustStorePassword != null && !urlContainsParam(jdbcUrl, "trustStorePassword")) {
                properties.put("trustStorePassword", rTrustStorePassword);
            }
        }
    }

    /**
     * Returns true if the mssql JDBC URL already contains the given parameter name
     * (matched case-insensitively in the ";name=" segment form).
     * When the URL specifies the parameter, the driver will use it directly and injecting
     * the same key into Properties would overwrite it (mssql-jdbc 13.x gives Properties precedence).
     */
    private static boolean urlContainsParam(String jdbcUrl, String paramName) {
        if (jdbcUrl == null) {
            return false;
        }
        var lowerUrl = jdbcUrl.toLowerCase(Locale.ROOT);
        var lowerParam = paramName.toLowerCase(Locale.ROOT);
        // mssql connection-string params appear as ;name= or ;name; (boolean shorthand)
        return lowerUrl.contains(";" + lowerParam + "=") || lowerUrl.contains(";" + lowerParam + ";");
    }
}
