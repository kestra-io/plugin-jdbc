package org.kestra.task.jdbc.clickhouse;

import io.micronaut.test.annotation.MicronautTest;
import org.kestra.task.jdbc.AbstractJdbcDriverTest;

import java.sql.Driver;

@MicronautTest
public class ClickHouseDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return ru.yandex.clickhouse.ClickHouseDriver.class;
    }
}
