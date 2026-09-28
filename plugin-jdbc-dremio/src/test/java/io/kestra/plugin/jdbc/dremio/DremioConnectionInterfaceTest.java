package io.kestra.plugin.jdbc.dremio;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class DremioConnectionInterfaceTest {
    @Test
    void registerDriverSetsNettyProperty() throws Exception {
        System.clearProperty("io.netty.tryReflectionSetAccessible");

        Query.builder().build().registerDriver();

        assertThat(System.getProperty("io.netty.tryReflectionSetAccessible"), is("true"));
    }
}
