package io.kestra.plugin.jdbc.redshift;

import io.kestra.plugin.jdbc.AbstractJdbcTriggerTest;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest(startRunner = true, startScheduler = true)
class RedshiftTriggerTest extends AbstractJdbcTriggerTest {
    @Value("${redshift.url:jdbc:redshift://127.0.0.1:55439/kestra}")
    protected String url;

    @Value("${redshift.user:postgres}")
    protected String user;

    @Value("${redshift.password:pg_passwd}")
    protected String password;
    @Test
    void run() throws Exception {
        var execution = triggerFlow(this.getClass().getClassLoader(), "flows","redshift-listen");

        var rows = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");
        assertThat(rows.size(), is(1));
    }

    @Override
    protected String getUrl() {
        return url;
    }

    @Override
    protected String getUsername() {
        return user;
    }

    @Override
    protected String getPassword() {
        return password;
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE DOMAIN super AS text");
        } catch (SQLException ignored) {
        }
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OR REPLACE FUNCTION json_parse(val text) RETURNS text LANGUAGE sql IMMUTABLE AS 'SELECT $1'");
        } catch (SQLException ignored) {
        }
        executeSqlScript("scripts/redshift.sql");
    }
}