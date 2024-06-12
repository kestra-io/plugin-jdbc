package io.kestra.plugin.jdbc.sybase;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;

import java.sql.Driver;

@KestraTest
public class SybaseDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return com.sybase.jdbc4.jdbc.SybDriver.class;
    }
}
