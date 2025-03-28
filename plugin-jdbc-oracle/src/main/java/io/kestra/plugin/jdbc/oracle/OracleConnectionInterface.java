package io.kestra.plugin.jdbc.oracle;

import io.kestra.plugin.jdbc.JdbcConnectionInterface;

public interface OracleConnectionInterface extends JdbcConnectionInterface {
    @Override
    default String getScheme() {
        return "jdbc:oracle";
    }
}
