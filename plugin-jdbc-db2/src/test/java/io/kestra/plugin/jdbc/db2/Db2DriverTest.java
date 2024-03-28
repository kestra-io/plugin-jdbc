package io.kestra.plugin.jdbc.db2;

import io.kestra.plugin.jdbc.AbstractJdbcDriverTest;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.BeforeAll;

import java.sql.Driver;

@MicronautTest
public class Db2DriverTest extends AbstractJdbcDriverTest {

    @BeforeAll
    static void initWait() {
	    try {
		    Thread.sleep(61000);
	    } catch (InterruptedException e) {
		    throw new RuntimeException(e);
	    }
    }

    @Override
    protected Class<? extends Driver> getDriverClass() {
        return com.ibm.db2.jcc.DB2Driver.class;
    }
}
