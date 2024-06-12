package io.kestra.plugin.jdbc.oracle;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;

import java.sql.Driver;

@KestraTest
public class OracleDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return oracle.jdbc.OracleDriver.class;
    }
}
