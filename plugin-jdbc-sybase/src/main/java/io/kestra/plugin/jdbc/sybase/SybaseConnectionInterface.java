package io.kestra.plugin.jdbc.sybase;

import io.kestra.plugin.jdbc.JdbcConnectionInterface;

public interface SybaseConnectionInterface extends JdbcConnectionInterface {
    @Override
    default String getScheme() {
        return "jdbc:sybase";
    }
}
