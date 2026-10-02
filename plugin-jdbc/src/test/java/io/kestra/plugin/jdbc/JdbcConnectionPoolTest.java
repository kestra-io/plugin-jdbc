package io.kestra.plugin.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

class JdbcConnectionPoolTest {
    private static final String URL = "jdbc:sqlserver://localhost:1433";

    private static Properties props(String encrypt) {
        var props = new Properties();
        props.setProperty("user", "sa");
        props.setProperty("password", "secret");
        props.setProperty("encrypt", encrypt);
        return props;
    }

    private static final String H2 = "jdbc:h2:mem:pooltest";

    private static Properties h2Props() {
        var props = new Properties();
        props.setProperty("user", "sa");
        props.setProperty("password", "");
        return props;
    }

    @AfterEach
    void cleanup() {
        JdbcConnectionPool.setIdleEvictionMs(JdbcConnectionPool.DEFAULT_IDLE_EVICTION_MS);
        JdbcConnectionPool.closeAll();
    }

    @Test
    void idlePoolIsEvictedAndRecreated() throws Exception {
        JdbcConnectionPool.connection(H2, h2Props(), 2).close();
        assertThat(JdbcConnectionPool.poolCount(), is(1));

        JdbcConnectionPool.setIdleEvictionMs(0);
        Thread.sleep(5);
        JdbcConnectionPool.evictIdlePools();
        assertThat(JdbcConnectionPool.poolCount(), is(0));

        try (var c = JdbcConnectionPool.connection(H2, h2Props(), 2)) {
            assertThat(c.isValid(1), is(true));
        }
        assertThat(JdbcConnectionPool.poolCount(), is(1));
    }

    @Test
    void poolWithBorrowedConnectionIsNotEvicted() throws Exception {
        try (var c = JdbcConnectionPool.connection(H2, h2Props(), 2)) {
            JdbcConnectionPool.setIdleEvictionMs(0);
            Thread.sleep(5);
            JdbcConnectionPool.evictIdlePools();
            assertThat(JdbcConnectionPool.poolCount(), greaterThan(0));
            assertThat(c.isValid(1), is(true));
        }
    }

    @Test
    void recentlyUsedPoolIsNotEvicted() throws Exception {
        JdbcConnectionPool.connection(H2, h2Props(), 2).close();
        JdbcConnectionPool.evictIdlePools();
        assertThat(JdbcConnectionPool.poolCount(), is(1));
    }

    @Test
    void differentDriverPropertiesProduceDifferentKeys() {
        assertThat(
            JdbcConnectionPool.poolKey(URL, props("true")),
            is(not(JdbcConnectionPool.poolKey(URL, props("false"))))
        );
    }

    @Test
    void identicalPropertiesProduceSameKey() {
        assertThat(
            JdbcConnectionPool.poolKey(URL, props("false")),
            is(JdbcConnectionPool.poolKey(URL, props("false")))
        );
    }
}
