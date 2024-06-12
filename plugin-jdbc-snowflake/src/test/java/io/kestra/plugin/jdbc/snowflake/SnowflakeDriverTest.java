package io.kestra.plugin.jdbc.snowflake;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;

import java.sql.Driver;

@KestraTest
public class SnowflakeDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return net.snowflake.client.jdbc.SnowflakeDriver.class;
    }
}
