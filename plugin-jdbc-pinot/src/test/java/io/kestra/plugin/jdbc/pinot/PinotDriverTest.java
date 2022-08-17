package io.kestra.plugin.jdbc.pinot;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;
import org.apache.pinot.client.PinotDriver;

import java.sql.Driver;

@MicronautTest
public class PinotDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return PinotDriver.class;
    }
}
