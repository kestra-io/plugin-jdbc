package io.kestra.plugin.jdbc.mysql;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;

import java.sql.Driver;

@KestraTest
public class MysqlDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return com.mysql.cj.jdbc.Driver.class;
    }
}
