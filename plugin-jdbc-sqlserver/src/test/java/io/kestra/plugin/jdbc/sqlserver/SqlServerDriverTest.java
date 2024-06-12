package io.kestra.plugin.jdbc.sqlserver;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;

import java.sql.Driver;

@KestraTest
public class SqlServerDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return com.microsoft.sqlserver.jdbc.SQLServerDriver.class;
    }
}
