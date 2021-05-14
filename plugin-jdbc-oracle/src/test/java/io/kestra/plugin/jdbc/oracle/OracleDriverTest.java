package io.kestra.plugin.jdbc.oracle;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;

import java.sql.Driver;

@MicronautTest
public class OracleDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return oracle.jdbc.OracleDriver.class;
    }
}
