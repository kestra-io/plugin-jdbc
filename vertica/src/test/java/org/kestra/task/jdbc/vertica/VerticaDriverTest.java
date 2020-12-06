package org.kestra.task.jdbc.vertica;

import io.micronaut.test.annotation.MicronautTest;
import org.kestra.task.jdbc.AbstractJdbcDriverTest;

import java.sql.Driver;

@MicronautTest
public class VerticaDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return com.vertica.jdbc.Driver.class;
    }
}
