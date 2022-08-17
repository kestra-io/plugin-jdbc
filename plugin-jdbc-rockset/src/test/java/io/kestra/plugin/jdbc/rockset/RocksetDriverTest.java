package io.kestra.plugin.jdbc.rockset;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;

import java.sql.Driver;

@MicronautTest
public class RocksetDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return com.rockset.jdbc.RocksetDriver.class;
    }
}
