package org.kestra.task.jdbc;

import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


@MicronautTest
public class TestJdbcDrivers {

    @Test
    public void testJdbcDriversLoaded() {

        List<Driver> drivers = Collections.list(DriverManager.getDrivers());

        assertThat(drivers, hasItems(
            hasProperty("class", is(org.postgresql.Driver.class)),
            hasProperty("class", is(com.mysql.cj.jdbc.Driver.class)),
            hasProperty("class", is(org.h2.Driver.class)) // used for test only
        ));
    }
}
