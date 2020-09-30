package org.kestra.task.jdbc.postgresql;

import io.micronaut.test.annotation.MicronautTest;
import org.kestra.task.jdbc.AbstractJdbcDriverTest;

import java.sql.Driver;

@MicronautTest
public class PgsqlDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return org.postgresql.Driver.class;
    }
}
