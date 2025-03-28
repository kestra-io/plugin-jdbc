package io.kestra.plugin.jdbc.db2;

import io.kestra.plugin.jdbc.JdbcConnectionInterface;

public interface Db2ConnectionInterface extends JdbcConnectionInterface {
    @Override
    default String getScheme() {
        return "jdbc:db2";
    }
}
