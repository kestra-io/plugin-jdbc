package io.kestra.plugin.jdbc.druid;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;

import java.sql.Driver;
@MicronautTest
public class DruidDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return org.apache.calcite.avatica.remote.Driver.class;
    }
}
