package io.kestra.plugin.jdbc.dremio;

import io.kestra.plugin.jdbc.JdbcConnectionInterface;

public interface DremioConnectionInterface extends JdbcConnectionInterface {
    @Override
    default String getScheme() {
        return "jdbc:dremio";
    }
}
