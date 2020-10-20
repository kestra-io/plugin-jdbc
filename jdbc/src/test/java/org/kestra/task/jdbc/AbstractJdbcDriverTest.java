package org.kestra.task.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public abstract class AbstractJdbcDriverTest {

    protected abstract Class<? extends Driver> getDriverClass();

    @Test
    public void loadDriver() {
        List<Driver> drivers = Collections.list(DriverManager.getDrivers());
        assertThat(drivers, hasItems(hasProperty("class", is(getDriverClass()))));
    }
}
