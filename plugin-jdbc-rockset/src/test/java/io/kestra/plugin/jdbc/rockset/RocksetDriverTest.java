package io.kestra.plugin.jdbc.rockset;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;
import org.junit.jupiter.api.Disabled;

import java.sql.Driver;

@KestraTest
@Disabled("Rocket didn't exist anymore")
public class RocksetDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return com.rockset.jdbc.RocksetDriver.class;
    }
}
