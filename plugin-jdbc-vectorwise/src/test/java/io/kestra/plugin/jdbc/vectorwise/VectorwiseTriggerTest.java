package io.kestra.plugin.jdbc.vectorwise;

import io.kestra.plugin.jdbc.AbstractJdbcTriggerTest;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
@Disabled("no server for unit test")
class VectorwiseTriggerTest extends AbstractJdbcTriggerTest {
    @Value("jdbc:ingres://url:port/db")
    protected String url;

    @Value("user")
    protected String user;

    @Value("password")
    protected String password;

    @Test
    void run() throws Exception {
        var execution = triggerFlow(this.getClass().getClassLoader(), "flows","vectorwise-listen");

        var rows = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");
        assertThat(rows.size(), is(1));
    }

    @Override
    protected String getUrl() {
        return this.url;
    }

    @Override
    protected String getUsername() {
        return this.user;
    }

    @Override
    protected String getPassword() {
        return this.password;
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/vectorwise.sql");
    }
}