package org.kestra.task.jdbc.mysql;

import io.micronaut.test.annotation.MicronautTest;
import org.kestra.task.jdbc.AbstractJdbcDriverTest;

import java.sql.Driver;

@MicronautTest
public class MysqlDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return com.mysql.cj.jdbc.Driver.class;
    }
}
