package io.kestra.plugin.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Shared JDBC connection pools keyed by (jdbcUrl, all connection properties).
 * All properties are included in the key so that connections with different settings
 * (credentials, SSL, ...) never share a pool.
 * Pools are created on first use and released on JVM shutdown or via closeAll().
 */
final class JdbcConnectionPool {

    private static final ConcurrentHashMap<String, HikariDataSource> POOLS = new ConcurrentHashMap<>();
    private static final AtomicBoolean SHUTDOWN_HOOK_REGISTERED = new AtomicBoolean(false);

    private JdbcConnectionPool() {}

    static Connection connection(String jdbcUrl, Properties props, int maxPoolSize) throws SQLException {
        var key = poolKey(jdbcUrl, props);
        var ds = POOLS.computeIfAbsent(key, k -> buildDataSource(jdbcUrl, props, maxPoolSize));
        return ds.getConnection();
    }

    static void closeAll() {
        POOLS.values().forEach(HikariDataSource::close);
        POOLS.clear();
    }

    static String poolKey(String jdbcUrl, Properties props) {
        // Concatenate the URL and every property with a separator (NUL) unlikely to appear in any
        // component, so connections with different settings (credentials, SSL, ...) never share a pool.
        var key = new StringBuilder(jdbcUrl);
        for (var name : new TreeSet<>(props.stringPropertyNames())) {
            key.append('\u0000').append(name).append('\u0000').append(props.getProperty(name));
        }
        return key.toString();
    }

    private static HikariDataSource buildDataSource(String jdbcUrl, Properties props, int maxPoolSize) {
        registerShutdownHook();

        var config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(props.getProperty("user"));
        config.setPassword(props.getProperty("password"));
        config.setMaximumPoolSize(maxPoolSize);
        // Release idle connections quickly to avoid pinning them on the worker.
        config.setMinimumIdle(0);
        config.setIdleTimeout(60_000);
        config.setMaxLifetime(1_800_000);

        // Short name for JMX / thread naming; derived from the URL without credentials.
        var shortKey = jdbcUrl.replaceAll("[^a-zA-Z0-9:._-]", "_");
        if (shortKey.length() > 40) {
            shortKey = shortKey.substring(0, 40);
        }
        config.setPoolName("kestra-jdbc-" + shortKey);

        // Pass any remaining driver-specific properties (ssl, applicationName, etc.).
        for (var entry : props.entrySet()) {
            var name = (String) entry.getKey();
            if (!"user".equals(name) && !"password".equals(name)) {
                config.addDataSourceProperty(name, entry.getValue());
            }
        }

        return new HikariDataSource(config);
    }

    private static void registerShutdownHook() {
        if (SHUTDOWN_HOOK_REGISTERED.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread(JdbcConnectionPool::closeAll, "kestra-jdbc-pool-shutdown"));
        }
    }
}
