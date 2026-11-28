package io.kestra.plugin.jdbc.openedge;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.plugin.jdbc.JdbcConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;

import java.sql.DriverManager;
import java.sql.SQLException;

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
        // DataDirect is proprietary and intentionally not a compile-time dependency.
        // Progress documents Class.forName as the supported way to load and register this driver.
        // Check first so repeated task executions do not register duplicate Driver instances.
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
            Class.forName(DRIVER_CLASS, true, classLoader);
        } catch (ClassNotFoundException | LinkageError e) {
            throw new SQLException(
                "Progress OpenEdge JDBC driver not found or could not be initialized. Add the licensed "
                    + "DataDirect openedge.jar and its required companion jars to the OpenEdge plugin classloader.",
                e
            );
        }
    }
}
