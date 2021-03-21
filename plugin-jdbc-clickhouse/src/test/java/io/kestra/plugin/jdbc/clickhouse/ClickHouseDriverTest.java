package io.kestra.plugin.jdbc.clickhouse;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;

import java.sql.Driver;

@MicronautTest
public class ClickHouseDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return ru.yandex.clickhouse.ClickHouseDriver.class;
    }
}
