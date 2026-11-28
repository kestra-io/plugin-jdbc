package io.kestra.plugin.jdbc.openedge;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

@KestraTest
class OpenEdgeConnectionInterfaceTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void exposesDataDirectUrlScheme() {
        OpenEdgeConnectionInterface connection = mock(OpenEdgeConnectionInterface.class, CALLS_REAL_METHODS);

        assertEquals("jdbc:datadirect:openedge", connection.getScheme());
    }

    @Test
    void acceptsDataDirectUrl() throws Exception {
        Query query = Query.builder()
            .url(Property.ofValue("jdbc:datadirect:openedge://localhost:20000;databaseName=sports2000"))
            .build();

        assertEquals(
            "jdbc:datadirect:openedge://localhost:20000;databaseName=sports2000",
            query.validateUrl(runContextFactory.of())
        );
    }

    @Test
    void rejectsOtherJdbcSchemes() {
        Query query = Query.builder()
            .url(Property.ofValue("jdbc:postgresql://localhost:5432/example"))
            .build();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> query.validateUrl(runContextFactory.of())
        );

        assertTrue(exception.getMessage().contains("jdbc:datadirect:openedge"));
    }

    @Test
    void registersDriverThroughJdbcServiceProvider() throws SQLException {
        OpenEdgeConnectionInterface connection = mock(OpenEdgeConnectionInterface.class, CALLS_REAL_METHODS);
        deregisterOpenEdgeDrivers();

        try {
            connection.registerDriver();

            assertTrue(DriverManager.drivers()
                .anyMatch(driver -> OpenEdgeConnectionInterface.DRIVER_CLASS.equals(driver.getClass().getName())));
        } finally {
            deregisterOpenEdgeDrivers();
        }
    }

    @Test
    void reportsMissingVendorDriverClearly() throws SQLException {
        OpenEdgeConnectionInterface connection = mock(OpenEdgeConnectionInterface.class, CALLS_REAL_METHODS);
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        deregisterOpenEdgeDrivers();

        try {
            // The platform classloader cannot see application/plugin jars, including the
            // proprietary driver that is intentionally absent from this project's test classpath.
            Thread.currentThread().setContextClassLoader(ClassLoader.getPlatformClassLoader());

            SQLException exception = assertThrows(SQLException.class, connection::registerDriver);

            assertTrue(exception.getMessage().contains("Progress OpenEdge JDBC driver not found"));
            assertTrue(exception.getMessage().contains("openedge.jar"));
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
            deregisterOpenEdgeDrivers();
        }
    }

    private static void deregisterOpenEdgeDrivers() throws SQLException {
        for (Driver driver : DriverManager.drivers()
            .filter(candidate -> OpenEdgeConnectionInterface.DRIVER_CLASS.equals(candidate.getClass().getName()))
            .toList()) {
            DriverManager.deregisterDriver(driver);
        }
    }
}
