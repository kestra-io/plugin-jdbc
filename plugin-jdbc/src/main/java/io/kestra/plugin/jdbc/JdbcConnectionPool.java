package io.kestra.plugin.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Shared JDBC connection pools keyed by (jdbcUrl, all connection properties).
 * All properties are included in the key so that connections with different settings
 * (credentials, SSL, ...) never share a pool.
 * Pools are created on first use and released on JVM shutdown or via closeAll().
 * A daemon sweeper also closes idle pools, so a key that changes on every run cannot leak pools.
 */
final class JdbcConnectionPool {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcConnectionPool.class);
    private static final ConcurrentHashMap<String, HikariDataSource> POOLS = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Long> LAST_USED = new ConcurrentHashMap<>();
    private static final AtomicBoolean SHUTDOWN_HOOK_REGISTERED = new AtomicBoolean(false);

    static final long DEFAULT_IDLE_EVICTION_MS = TimeUnit.MINUTES.toMillis(10);
    private static final long SWEEP_INTERVAL_SECONDS = 60;

    private static volatile long idleEvictionMs = DEFAULT_IDLE_EVICTION_MS;
    private static ScheduledExecutorService sweeper;

    private JdbcConnectionPool() {}

    static Connection connection(String jdbcUrl, Properties props, int maxPoolSize) throws SQLException {
        var key = poolKey(jdbcUrl, props);
        startSweeper();
        var ds = acquire(key, jdbcUrl, props, maxPoolSize);
        try {
            return ds.getConnection();
        } catch (SQLException e) {
            // The sweeper or closeAll() closed the pool between lookup and getConnection(): retry once with a fresh one.
            if (ds.isClosed()) {
                return acquire(key, jdbcUrl, props, maxPoolSize).getConnection();
            }
            throw e;
        } finally {
            LAST_USED.put(key, System.currentTimeMillis());
        }
    }

    private static HikariDataSource acquire(String key, String jdbcUrl, Properties props, int maxPoolSize) {
        return POOLS.compute(key, (k, existing) -> {
            var pool = existing == null || existing.isClosed() ? buildDataSource(jdbcUrl, props, maxPoolSize) : existing;
            // Updated inside compute() so the sweeper, which also holds the key lock, never sees a stale timestamp.
            LAST_USED.put(k, System.currentTimeMillis());
            return pool;
        });
    }

    static void closeAll() {
        POOLS.values().forEach(HikariDataSource::close);
        POOLS.clear();
        LAST_USED.clear();
    }

    static int poolCount() {
        return POOLS.size();
    }

    static void setIdleEvictionMs(long value) {
        idleEvictionMs = value;
    }

    /** Closes pools without borrowed connection that were not used for the idle eviction delay. */
    static void evictIdlePools() {
        var threshold = System.currentTimeMillis() - idleEvictionMs;
        for (var key : POOLS.keySet()) {
            // compute() is atomic per key, so a concurrent connection(...) either runs before (and refreshes
            // LAST_USED / holds a connection) or after (and builds a fresh pool).
            try {
                POOLS.computeIfPresent(key, (k, ds) -> {
                    var last = LAST_USED.getOrDefault(k, 0L);
                    var mx = ds.getHikariPoolMXBean();
                    var active = mx == null ? 0 : mx.getActiveConnections();
                    if (ds.isClosed()) {
                        LAST_USED.remove(k);
                        return null;
                    }
                    if (active == 0 && last <= threshold) {
                        LAST_USED.remove(k);
                        ds.close();
                        return null;
                    }
                    return ds;
                });
            } catch (Exception e) {
                // One failing pool must not stop the sweep of the others.
                LOG.warn("Failed to evict idle JDBC pool", e);
            }
        }
    }

    private static synchronized void startSweeper() {
        if (sweeper != null) {
            return;
        }
        sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "kestra-jdbc-pool-evictor");
            t.setDaemon(true);
            return t;
        });
        sweeper.scheduleWithFixedDelay(() -> {
            try {
                evictIdlePools();
            } catch (Exception e) {
                // An uncaught exception would cancel the periodic task.
                LOG.warn("JDBC pool sweep failed", e);
            }
        }, SWEEP_INTERVAL_SECONDS, SWEEP_INTERVAL_SECONDS, TimeUnit.SECONDS);
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
