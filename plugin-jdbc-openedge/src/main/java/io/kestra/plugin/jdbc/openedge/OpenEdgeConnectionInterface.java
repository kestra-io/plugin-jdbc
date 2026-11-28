package io.kestra.plugin.jdbc.openedge;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.plugin.jdbc.JdbcConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

public interface OpenEdgeConnectionInterface extends JdbcConnectionInterface {
    String DRIVER_CLASS = "com.ddtek.jdbc.openedge.OpenEdgeDriver";

    @Override
    @Schema(title = "The database user's password")
    @PluginProperty(secret = true, group = "connection")
    Property<String> getPassword();

    @Override
    default String getScheme() {
        return "jdbc:datadirect:openedge";
    }

    @Override
    default void registerDriver() throws SQLException {
        // The proprietary driver is intentionally not a compile-time dependency.
        // Discover its JDBC 4 service provider from the plugin classloader instead of
        // reflectively loading a class or accepting a user-controlled class name/path.
        boolean alreadyRegistered = DriverManager.drivers()
            .anyMatch(driver -> DRIVER_CLASS.equals(driver.getClass().getName()));

        if (alreadyRegistered) {
            return;
        }

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = getClass().getClassLoader();
        }

        try {
            Driver driver = ServiceLoader.load(Driver.class, classLoader)
                .stream()
                .filter(provider -> DRIVER_CLASS.equals(provider.type().getName()))
                .map(ServiceLoader.Provider::get)
                .findFirst()
                .orElseThrow(() -> new SQLException(
                    "Progress OpenEdge JDBC driver not found. Add the licensed DataDirect openedge.jar "
                        + "and its required companion jars to the OpenEdge plugin classloader; the driver "
                        + "must expose the standard JDBC service provider."
                ));

            DriverManager.registerDriver(driver);
        } catch (ServiceConfigurationError | LinkageError e) {
            throw new SQLException(
                "Progress OpenEdge JDBC driver could not be loaded. Verify that the licensed DataDirect "
                    + "openedge.jar and its required companion jars are available in the OpenEdge plugin classloader.",
                e
            );
        }
    }
}
