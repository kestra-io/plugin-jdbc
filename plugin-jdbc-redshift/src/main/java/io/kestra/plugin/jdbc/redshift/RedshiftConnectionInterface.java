package io.kestra.plugin.jdbc.redshift;

import io.kestra.plugin.jdbc.JdbcConnectionInterface;

public interface RedshiftConnectionInterface extends JdbcConnectionInterface {
    @Override
    default String getScheme() {
        return "jdbc:redshift";
    }
}
