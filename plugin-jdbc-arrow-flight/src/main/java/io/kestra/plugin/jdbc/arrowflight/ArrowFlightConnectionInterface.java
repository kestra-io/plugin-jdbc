package io.kestra.plugin.jdbc.arrowflight;

import io.kestra.plugin.jdbc.JdbcConnectionInterface;

public interface ArrowFlightConnectionInterface extends JdbcConnectionInterface {
    @Override
    default String getScheme() {
        return "jdbc:arrow-flight-sql";
    }
}
