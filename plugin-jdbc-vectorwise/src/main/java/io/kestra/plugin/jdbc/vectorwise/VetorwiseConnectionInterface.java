package io.kestra.plugin.jdbc.vectorwise;

import io.kestra.plugin.jdbc.JdbcConnectionInterface;

public interface VetorwiseConnectionInterface extends JdbcConnectionInterface {
    @Override
    default String getScheme() {
        return "jdbc:vectorwise";
    }
}
