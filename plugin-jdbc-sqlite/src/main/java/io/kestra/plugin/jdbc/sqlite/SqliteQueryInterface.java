package io.kestra.plugin.jdbc.sqlite;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.plugin.jdbc.JdbcConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;

import java.sql.DriverManager;
import java.sql.SQLException;

public interface SqliteQueryInterface extends JdbcConnectionInterface {
    @PluginProperty(group = "connection")
    @Schema(
        title = "Add sqlite file.",
        description = "The file must be from Kestra's internal storage"
    )
    Property<String> getSqliteFile();

    @Schema(
        title = "Output the database file.",
        description = "This property lets you define if you want to output the provided `sqliteFile` database file for further processing."
    )
    Property<Boolean> getOutputDbFile();


    default void registerDriver() throws SQLException {
        DriverManager.registerDriver(new org.sqlite.JDBC());
    }

    @Override
    default String getScheme() {
        return "jdbc:sqlite";
    }
}
