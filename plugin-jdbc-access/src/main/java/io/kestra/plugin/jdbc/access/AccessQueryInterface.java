package io.kestra.plugin.jdbc.access;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.plugin.jdbc.JdbcConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;

import java.sql.DriverManager;
import java.sql.SQLException;

public interface AccessQueryInterface extends JdbcConnectionInterface {

    default void registerDriver() throws SQLException {
        DriverManager.registerDriver(new net.ucanaccess.jdbc.UcanaccessDriver());
    }

    @Override
    default String getScheme() {
        return "jdbc:ucanaccess";
    }

    // MS Access is a file-based, in-process database (UCanAccess); pooling causes file-lock conflicts.
    @Override
    default boolean usesConnectionPool() {
        return false;
    }
}
