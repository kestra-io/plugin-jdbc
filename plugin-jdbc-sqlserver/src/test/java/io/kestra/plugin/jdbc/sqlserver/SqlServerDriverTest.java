package io.kestra.plugin.jdbc.sqlserver;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;

import java.sql.Driver;

@MicronautTest
public class SqlServerDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return com.microsoft.sqlserver.jdbc.SQLServerDriver.class;
    }
}
