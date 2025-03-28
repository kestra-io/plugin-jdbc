package io.kestra.plugin.jdbc.clickhouse;

import io.kestra.plugin.jdbc.JdbcConnectionInterface;

public interface ClickhouseConnectionInterface extends JdbcConnectionInterface {
    @Override
    default String getScheme() {
        return "jdbc:clickhouse";
    }
}
