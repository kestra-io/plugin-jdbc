package io.kestra.plugin.jdbc;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
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
