package io.kestra.plugin.jdbc.as400;

import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;
import io.kestra.core.junit.annotations.KestraTest;

import java.sql.Driver;

@KestraTest
public class As400DriverTest extends AbstractJdbcDriverTest {

    @Override
    protected Class<? extends Driver> getDriverClass() {
        return com.ibm.as400.access.AS400JDBCDriver.class;
    }
}
