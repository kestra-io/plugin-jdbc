package io.kestra.plugin.jdbc.sqlite;

import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;
import io.kestra.core.junit.annotations.KestraTest;

import java.sql.Driver;

@KestraTest
class SqliteDriverTest extends AbstractJdbcDriverTest {

    @Override
    protected Class<? extends Driver> getDriverClass() {
        return org.sqlite.JDBC.class;
    }
}