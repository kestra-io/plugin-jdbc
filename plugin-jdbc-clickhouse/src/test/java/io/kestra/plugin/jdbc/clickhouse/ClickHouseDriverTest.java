package io.kestra.plugin.jdbc.clickhouse;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;

import java.sql.Driver;

@KestraTest
public class ClickHouseDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return com.clickhouse.jdbc.ClickHouseDriver.class;
    }
}
