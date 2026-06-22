package io.kestra.plugin.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Shared JDBC connection pools keyed by (jdbcUrl, all driver properties).
 * All driver properties are included in the key so that pools with different
 * SSL or connection settings are never shared across distinct configurations.
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
        var sorted = new TreeMap<String, String>();
        for (var name : props.stringPropertyNames()) {
            sorted.put(name, props.getProperty(name));
        }
        return jdbcUrl + "|" + sorted;
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

        var poolName = "kestra-jdbc-" + Integer.toUnsignedString(poolKey(jdbcUrl, props).hashCode(), 16);
        config.setPoolName(poolName);

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
