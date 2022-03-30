package io.kestra.plugin.jdbc.snowflake;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;

import java.sql.Driver;

@MicronautTest
public class SnowflakeDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return net.snowflake.client.jdbc.SnowflakeDriver.class;
    }
}
