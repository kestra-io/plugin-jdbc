package io.kestra.plugin.jdbc.sqlserver;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

import java.util.Locale;
import java.util.Properties;
import java.util.function.Function;

public abstract class SqlServerService {
    public static void handleSsl(Properties properties, RunContext runContext, SqlServerConnectionInterface conn) throws Exception {
        var jdbcUrl = properties.getProperty("jdbc.url");

        putUnlessInUrl(properties, jdbcUrl, "encrypt",
            render(runContext, conn.getEncrypt(), SqlServerConnectionInterface.EncryptMode.class, e -> e.name().toLowerCase(Locale.ROOT)));
        putUnlessInUrl(properties, jdbcUrl, "trustServerCertificate",
            render(runContext, conn.getTrustServerCertificate(), Boolean.class, Object::toString));
        putUnlessInUrl(properties, jdbcUrl, "hostNameInCertificate",
            render(runContext, conn.getHostNameInCertificate(), String.class, Function.identity()));
        putUnlessInUrl(properties, jdbcUrl, "trustStore",
            render(runContext, conn.getTrustStore(), String.class, Function.identity()));
        putUnlessInUrl(properties, jdbcUrl, "trustStorePassword",
            render(runContext, conn.getTrustStorePassword(), String.class, Function.identity()));
    }

    private static <T> String render(RunContext runContext, Property<T> property, Class<T> type, Function<T, String> format) throws IllegalVariableEvaluationException {
        if (property == null) {
            return null;
        }
        var value = runContext.render(property).as(type).orElse(null);
        return value == null ? null : format.apply(value);
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
