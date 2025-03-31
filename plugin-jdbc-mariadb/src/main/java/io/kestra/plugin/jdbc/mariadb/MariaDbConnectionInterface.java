package io.kestra.plugin.jdbc.mariadb;

import io.kestra.plugin.jdbc.JdbcConnectionInterface;

public interface MariaDbConnectionInterface extends JdbcConnectionInterface {
    @Override
    default String getScheme() {
        return "jdbc:mariadb";
    }
}
