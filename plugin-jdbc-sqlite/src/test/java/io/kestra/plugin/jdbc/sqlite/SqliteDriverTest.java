package io.kestra.plugin.jdbc.sqlite;

import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;

import java.sql.Driver;

@MicronautTest
class SqliteDriverTest extends AbstractJdbcDriverTest {

    @Override
    protected Class<? extends Driver> getDriverClass() {
        return org.sqlite.JDBC.class;
    }
}