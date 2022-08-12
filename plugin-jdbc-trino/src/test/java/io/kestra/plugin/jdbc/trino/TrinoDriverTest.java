package io.kestra.plugin.jdbc.trino;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;

import java.sql.Driver;

@MicronautTest
public class TrinoDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return io.trino.jdbc.TrinoDriver.class;
    }
}
