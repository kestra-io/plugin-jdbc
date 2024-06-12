package io.kestra.plugin.jdbc.db2;

import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;
import io.kestra.core.junit.annotations.KestraTest;

import java.sql.Driver;

@KestraTest
public class Db2DriverTest extends AbstractJdbcDriverTest {

    @Override
    protected Class<? extends Driver> getDriverClass() {
        return com.ibm.db2.jcc.DB2Driver.class;
    }
}
