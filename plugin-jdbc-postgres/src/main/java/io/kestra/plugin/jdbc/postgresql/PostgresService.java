package io.kestra.plugin.jdbc.postgresql;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Properties;
import java.util.function.Function;

public abstract class PostgresService {
    public static void handleSsl(Properties properties, RunContext runContext, PostgresConnectionInterface conn, Function<byte[], Path> temp) throws IllegalVariableEvaluationException, IOException {
        if (conn.getSsl() != null && conn.getSsl()) {
            properties.put("ssl", "true");
        }

        if (conn.getSslMode() != null) {
            properties.put("sslmode", conn.getSslMode().name().toUpperCase(Locale.ROOT).replace("_", "-"));
        }

        if (conn.getSslRootCert() != null) {
            properties.put("sslrootcert", temp.apply(runContext.render(conn.getSslRootCert()).getBytes(StandardCharsets.UTF_8)).toAbsolutePath().toString());
        }

        if (conn.getSslCert() != null) {
            properties.put("sslcert", temp.apply(runContext.render(conn.getSslCert()).getBytes(StandardCharsets.UTF_8)).toAbsolutePath().toString());
        }

        if (conn.getSslKey() != null) {
            properties.put("sslkey", temp.apply(runContext.render(conn.getSslKey()).getBytes(StandardCharsets.UTF_8)).toAbsolutePath().toString());
        }

        if (conn.getSslKeyPassword() != null) {
            properties.put("sslpassword", runContext.render(conn.getSslKeyPassword()));
        }
    }
}
