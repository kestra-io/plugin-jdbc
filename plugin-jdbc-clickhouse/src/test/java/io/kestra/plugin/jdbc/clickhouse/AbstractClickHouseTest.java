package io.kestra.plugin.jdbc.clickhouse;

import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.sql.SQLException;

public abstract class AbstractClickHouseTest extends AbstractRdbmsTest  {

    @Override
    protected String getUsername() {
        return "myuser";
    }

    @Override
    protected String getPassword() {
        return "mypassword";
    }

    @Override
    protected String getUrl() {
        return "jdbc:clickhouse://127.0.0.1:28123/default";
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/clickhouse_queries.sql");
    }

}
