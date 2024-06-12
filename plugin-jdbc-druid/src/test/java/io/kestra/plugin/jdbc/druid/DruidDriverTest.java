package io.kestra.plugin.jdbc.druid;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;

import java.sql.Driver;
@KestraTest
public class DruidDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return org.apache.calcite.avatica.remote.Driver.class;
    }
}
