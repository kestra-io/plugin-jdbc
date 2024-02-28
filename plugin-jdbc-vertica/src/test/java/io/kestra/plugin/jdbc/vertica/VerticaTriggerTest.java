package io.kestra.plugin.jdbc.vertica;

import io.kestra.plugin.jdbc.AbstractJdbcTriggerTest;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class VerticaTriggerTest extends AbstractJdbcTriggerTest {
    @Test
    void run() throws Exception {
        var execution = triggerFlow(this.getClass().getClassLoader(), "flows","vertica-listen");

        var rows = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");
        assertThat(rows.size(), is(1));
    }
    @Override
    protected String getUrl() {
        return "jdbc:vertica://127.0.0.1:25433/docker";
    }

    @Override
    protected String getUsername() {
        return "dbadmin";
    }

    @Override
    protected String getPassword() {
        return "vertica_passwd";
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/vertica.sql");
    }
}