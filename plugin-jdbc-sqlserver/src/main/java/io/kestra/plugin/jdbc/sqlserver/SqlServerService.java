package io.kestra.plugin.jdbc.sqlserver;

import io.kestra.core.runners.RunContext;

import java.util.Locale;
import java.util.Properties;

public abstract class SqlServerService {
    public static void handleSsl(Properties properties, RunContext runContext, SqlServerConnectionInterface conn) throws Exception {
        if (conn.getEncrypt() != null) {
            var rEncrypt = runContext.render(conn.getEncrypt()).as(SqlServerConnectionInterface.EncryptMode.class).orElse(null);
            if (rEncrypt != null) {
                properties.put("encrypt", rEncrypt.name().toLowerCase(Locale.ROOT));
            }
        }

        if (conn.getTrustServerCertificate() != null) {
            var rTrustServerCertificate = runContext.render(conn.getTrustServerCertificate()).as(Boolean.class).orElse(null);
            if (rTrustServerCertificate != null) {
                properties.put("trustServerCertificate", rTrustServerCertificate.toString());
            }
        }

        if (conn.getHostNameInCertificate() != null) {
            var rHostNameInCertificate = runContext.render(conn.getHostNameInCertificate()).as(String.class).orElse(null);
            if (rHostNameInCertificate != null) {
                properties.put("hostNameInCertificate", rHostNameInCertificate);
            }
        }

        if (conn.getTrustStore() != null) {
            var rTrustStore = runContext.render(conn.getTrustStore()).as(String.class).orElse(null);
            if (rTrustStore != null) {
                properties.put("trustStore", rTrustStore);
            }
        }

        if (conn.getTrustStorePassword() != null) {
            var rTrustStorePassword = runContext.render(conn.getTrustStorePassword()).as(String.class).orElse(null);
            if (rTrustStorePassword != null) {
                properties.put("trustStorePassword", rTrustStorePassword);
            }
        }
    }
}
