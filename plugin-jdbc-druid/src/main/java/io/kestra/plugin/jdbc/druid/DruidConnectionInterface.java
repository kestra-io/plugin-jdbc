package io.kestra.plugin.jdbc.druid;

import io.kestra.plugin.jdbc.JdbcConnectionInterface;

public interface DruidConnectionInterface extends JdbcConnectionInterface {
    @Override
    default String getScheme() {
        return "jdbc:avatica";
    }
}
