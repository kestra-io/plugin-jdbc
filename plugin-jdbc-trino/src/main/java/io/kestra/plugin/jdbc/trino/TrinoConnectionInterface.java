package io.kestra.plugin.jdbc.trino;

import io.kestra.plugin.jdbc.JdbcConnectionInterface;

public interface TrinoConnectionInterface extends JdbcConnectionInterface {
    @Override
    default String getScheme() {
        return "jdbc:trino";
    }
}
