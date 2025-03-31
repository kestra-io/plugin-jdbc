package io.kestra.plugin.jdbc.vertica;

import io.kestra.plugin.jdbc.JdbcConnectionInterface;

public interface VerticaConnectionInterface extends JdbcConnectionInterface {
    @Override
    default String getScheme() {
        return "jdbc:vertica";
    }
}
