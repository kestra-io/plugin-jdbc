package io.kestra.plugin.jdbc.sqlserver;

import io.kestra.plugin.jdbc.JdbcConnectionInterface;

public interface SqlServerConnectionInterface extends JdbcConnectionInterface {
    @Override
    default String getScheme() {
        return "jdbc:sqlserver";
    }
}
