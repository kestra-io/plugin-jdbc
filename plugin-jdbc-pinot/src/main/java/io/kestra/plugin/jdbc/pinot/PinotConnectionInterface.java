package io.kestra.plugin.jdbc.pinot;

import io.kestra.plugin.jdbc.JdbcConnectionInterface;

public interface PinotConnectionInterface extends JdbcConnectionInterface {
    @Override
    default String getScheme() {
        return "jdbc:pinot";
    }
}
