package io.kestra.plugin.jdbc.druid;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;
import org.junit.jupiter.api.Disabled;

import java.sql.Driver;

@Disabled("Druid cluster takes too long to start in CI")
@KestraTest
public class DruidDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return org.apache.calcite.avatica.remote.Driver.class;
    }
}
