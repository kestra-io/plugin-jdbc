package io.kestra.plugin.jdbc.pinot;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;
import org.apache.pinot.client.PinotDriver;

import java.sql.Driver;

@KestraTest
public class PinotDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return PinotDriver.class;
    }
}
