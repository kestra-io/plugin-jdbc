package io.kestra.plugin.jdbc.duckdb;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;

import java.sql.Driver;

@MicronautTest
public class DuckDbDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return org.duckdb.DuckDBDriver.class;
    }
}
