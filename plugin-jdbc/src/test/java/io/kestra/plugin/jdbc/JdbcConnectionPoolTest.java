package io.kestra.plugin.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * BUG 2 regression: JdbcConnectionPool must include driver properties in the pool key.
 * Before the fix, two calls with identical url/user/password but different SSL props
 * reused the stale first pool, silently ignoring the second call's property values.
 */
class JdbcConnectionPoolTest {

    private static final String FAKE_URL = "jdbc:fake-pool-test://localhost/db";

    private final CopyOnWriteArrayList<Properties> capturedProps = new CopyOnWriteArrayList<>();
    private FakeDriver fakeDriver;

    @BeforeEach
    void registerFakeDriver() throws SQLException {
        fakeDriver = new FakeDriver(capturedProps);
        DriverManager.registerDriver(fakeDriver);
    }

    @AfterEach
    void cleanup() throws SQLException {
        try {
            JdbcConnectionPool.closeAll();
        } finally {
            DriverManager.deregisterDriver(fakeDriver);
        }
    }

    /**
     * BUG 2: same url/user/password but different driver property (encrypt) must produce
     * separate pools. Before the fix, computeIfAbsent reuses the first pool for the second
     * call, so only 1 pool exists after 2 calls with different properties.
     */
    @Test
    void differentDriverPropertiesProduceSeparatePools() throws SQLException {
        var propsA = new Properties();
        propsA.setProperty("user", "sa");
        propsA.setProperty("password", "secret");
        propsA.setProperty("encrypt", "false");

        var propsB = new Properties();
        propsB.setProperty("user", "sa");
        propsB.setProperty("password", "secret");
        propsB.setProperty("encrypt", "true");

        try (Connection ignored = JdbcConnectionPool.connection(FAKE_URL, propsA, 1)) {}
        try (Connection ignored = JdbcConnectionPool.connection(FAKE_URL, propsB, 1)) {}

        // BUG 2: before the fix, only 1 pool is created (second call reuses the stale first pool).
        // After the fix, 2 separate pools must exist.
        assertThat(
            "each distinct property set must produce a separate pool",
            JdbcConnectionPool.poolCount(), is(2)
        );
    }

    @Test
    void identicalPropertiesReusePool() throws SQLException {
        var propsA = new Properties();
        propsA.setProperty("user", "sa");
        propsA.setProperty("password", "secret");
        propsA.setProperty("encrypt", "false");

        var propsB = new Properties();
        propsB.setProperty("user", "sa");
        propsB.setProperty("password", "secret");
        propsB.setProperty("encrypt", "false");

        try (Connection ignored = JdbcConnectionPool.connection(FAKE_URL, propsA, 1)) {}
        try (Connection ignored = JdbcConnectionPool.connection(FAKE_URL, propsB, 1)) {}

        // Same properties: exactly 1 pool must exist after both calls.
        assertThat("identical properties must reuse the same pool", JdbcConnectionPool.poolCount(), is(1));
    }

    @Test
    void propertiesArePassedToDriverConnect() throws SQLException {
        var props = new Properties();
        props.setProperty("user", "sa");
        props.setProperty("password", "secret");
        props.setProperty("encrypt", "true");

        try (Connection ignored = JdbcConnectionPool.connection(FAKE_URL, props, 1)) {}

        // At least one connect() call must have happened, and encrypt must be "true"
        assertThat("driver.connect must be called at least once", capturedProps.isEmpty(), is(false));
        assertThat("encrypt must be forwarded to driver",
            capturedProps.getFirst().getProperty("encrypt"), is("true"));
    }

    // Minimal Driver that accepts our fake URL and records connect() calls.
    private static final class FakeDriver implements Driver {

        private final CopyOnWriteArrayList<Properties> capturedProps;

        FakeDriver(CopyOnWriteArrayList<Properties> capturedProps) {
            this.capturedProps = capturedProps;
        }

        @Override
        public Connection connect(String url, Properties info) {
            capturedProps.add(new Properties(info));
            return new NoOpConnection();
        }

        @Override
        public boolean acceptsURL(String url) {
            return url != null && url.startsWith("jdbc:fake-pool-test:");
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() { return 1; }

        @Override
        public int getMinorVersion() { return 0; }

        @Override
        public boolean jdbcCompliant() { return false; }

        @Override
        public Logger getParentLogger() { return Logger.getLogger(FakeDriver.class.getName()); }
    }

    // Minimal Connection stub required by HikariCP.
    private static final class NoOpConnection implements Connection {
        @Override public java.sql.Statement createStatement() { return null; }
        @Override public java.sql.PreparedStatement prepareStatement(String sql) { return null; }
        @Override public java.sql.CallableStatement prepareCall(String sql) { return null; }
        @Override public String nativeSQL(String sql) { return sql; }
        @Override public void setAutoCommit(boolean autoCommit) {}
        @Override public boolean getAutoCommit() { return true; }
        @Override public void commit() {}
        @Override public void rollback() {}
        @Override public void close() {}
        @Override public boolean isClosed() { return false; }
        @Override public java.sql.DatabaseMetaData getMetaData() { return null; }
        @Override public void setReadOnly(boolean readOnly) {}
        @Override public boolean isReadOnly() { return false; }
        @Override public void setCatalog(String catalog) {}
        @Override public String getCatalog() { return null; }
        @Override public void setTransactionIsolation(int level) {}
        @Override public int getTransactionIsolation() { return TRANSACTION_NONE; }
        @Override public java.sql.SQLWarning getWarnings() { return null; }
        @Override public void clearWarnings() {}
        @Override public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency) { return null; }
        @Override public java.sql.PreparedStatement prepareStatement(String sql, int rsc, int rcc) { return null; }
        @Override public java.sql.CallableStatement prepareCall(String sql, int rsc, int rcc) { return null; }
        @Override public java.util.Map<String, Class<?>> getTypeMap() { return null; }
        @Override public void setTypeMap(java.util.Map<String, Class<?>> map) {}
        @Override public void setHoldability(int holdability) {}
        @Override public int getHoldability() { return 0; }
        @Override public java.sql.Savepoint setSavepoint() { return null; }
        @Override public java.sql.Savepoint setSavepoint(String name) { return null; }
        @Override public void rollback(java.sql.Savepoint savepoint) {}
        @Override public void releaseSavepoint(java.sql.Savepoint savepoint) {}
        @Override public java.sql.Statement createStatement(int rsc, int rcc, int rh) { return null; }
        @Override public java.sql.PreparedStatement prepareStatement(String sql, int rsc, int rcc, int rh) { return null; }
        @Override public java.sql.CallableStatement prepareCall(String sql, int rsc, int rcc, int rh) { return null; }
        @Override public java.sql.PreparedStatement prepareStatement(String sql, int[] columnIndexes) { return null; }
        @Override public java.sql.PreparedStatement prepareStatement(String sql, String[] columnNames) { return null; }
        @Override public java.sql.PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) { return null; }
        @Override public java.sql.Clob createClob() { return null; }
        @Override public java.sql.Blob createBlob() { return null; }
        @Override public java.sql.NClob createNClob() { return null; }
        @Override public java.sql.SQLXML createSQLXML() { return null; }
        @Override public boolean isValid(int timeout) { return true; }
        @Override public void setClientInfo(String name, String value) {}
        @Override public void setClientInfo(java.util.Properties properties) {}
        @Override public String getClientInfo(String name) { return null; }
        @Override public java.util.Properties getClientInfo() { return new java.util.Properties(); }
        @Override public java.sql.Array createArrayOf(String typeName, Object[] elements) { return null; }
        @Override public java.sql.Struct createStruct(String typeName, Object[] attributes) { return null; }
        @Override public void setSchema(String schema) {}
        @Override public String getSchema() { return null; }
        @Override public void abort(java.util.concurrent.Executor executor) {}
        @Override public void setNetworkTimeout(java.util.concurrent.Executor executor, int milliseconds) {}
        @Override public int getNetworkTimeout() { return 0; }
        @Override public <T> T unwrap(Class<T> iface) { return null; }
        @Override public boolean isWrapperFor(Class<?> iface) { return false; }
    }
}
