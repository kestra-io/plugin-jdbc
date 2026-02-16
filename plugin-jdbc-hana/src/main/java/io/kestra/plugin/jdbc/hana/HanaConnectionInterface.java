package io.kestra.plugin.jdbc.hana;

import io.kestra.plugin.jdbc.JdbcConnectionInterface;

public interface HanaConnectionInterface extends JdbcConnectionInterface {

    @Override
    default String getScheme() {
        return "jdbc:sap";
    }
}
