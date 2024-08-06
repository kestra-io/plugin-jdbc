package io.kestra.plugin.jdbc.vertica;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;
import org.junit.jupiter.api.Disabled;

import java.sql.Driver;

@KestraTest
@Disabled("Issue with EOFException")
public class VerticaDriverTest extends AbstractJdbcDriverTest {
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return com.vertica.jdbc.Driver.class;
    }
}
