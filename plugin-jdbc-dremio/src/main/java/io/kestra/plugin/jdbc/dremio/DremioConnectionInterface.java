package io.kestra.plugin.jdbc.dremio;

import com.dremio.jdbc.Driver;
import io.kestra.plugin.jdbc.JdbcConnectionInterface;

import java.sql.DriverManager;
import java.sql.SQLException;

public interface DremioConnectionInterface extends JdbcConnectionInterface {
    @Override
    default String getScheme() {
        return "jdbc:dremio";
    }

    @Override
    default void registerDriver() throws SQLException {
        // Netty needs this to use direct buffers on JDK9+, else TLS handshake fails
        System.setProperty("io.netty.tryReflectionSetAccessible", "true");

        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(Driver.class::isInstance)) {
            DriverManager.registerDriver(new Driver());
        }
    }
}
