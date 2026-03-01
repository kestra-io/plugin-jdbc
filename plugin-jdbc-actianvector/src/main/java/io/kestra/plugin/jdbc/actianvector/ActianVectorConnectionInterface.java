package io.kestra.plugin.jdbc.actianvector;

import io.kestra.plugin.jdbc.JdbcConnectionInterface;

public interface ActianVectorConnectionInterface extends JdbcConnectionInterface {
    @Override
    default String getScheme() {
        return "jdbc:vectorwise";
    }
}
