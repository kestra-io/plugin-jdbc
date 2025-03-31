package io.kestra.plugin.jdbc.as400;

import io.kestra.plugin.jdbc.JdbcConnectionInterface;

public interface As400ConnectionInterface extends JdbcConnectionInterface {
    @Override
    default String getScheme() {
        return "jdbc:as400";
    }
}
