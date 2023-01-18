package io.kestra.plugin.jdbc.snowflake;

import io.kestra.plugin.jdbc.AbstractJdbcTriggerTest;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
@Disabled("no server for unit test")
class SnowflakeTriggerTest extends AbstractJdbcTriggerTest {
    @Value("${snowflake.host}")
    protected String host;

    @Value("${snowflake.username}")
    protected String username;

    @Value("${snowflake.password}")
    protected String password;
    @Test
    void run() throws Exception {
        var execution = triggerFlow(this.getClass().getClassLoader(), "flows","snowflake-listen");

        var rows = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");
        assertThat(rows.size(), is(1));
    }

    @Override
    protected String getUrl() {
        return "jdbc:snowflake://" + this.host + "/?loginTimeout=3";
    }

    @Override
    protected String getUsername() {
        return this.username;
    }

    @Override
    protected String getPassword() {
        return this.password;
    }

    @Override
    protected Connection getConnection() throws SQLException {
        Properties properties = new Properties();
        properties.put("user", getUsername());
        properties.put("password", getPassword());
        properties.put("warehouse", "COMPUTE_WH");
        properties.put("db", "UNITTEST");
        properties.put("schema", "public");

        return DriverManager.getConnection(getUrl(), properties);
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/snowflake.sql");
    }
}