package io.kestra.plugin.jdbc.db2;

import io.kestra.plugin.jdbc.AbstractJdbcTriggerTest;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
@DisabledIf(
    value = "isDisabled",
    disabledReason = "The tests are disabled for CI, as db2 container have long time initialization"
)
class Db2TriggerTest extends AbstractJdbcTriggerTest {

    static boolean isDisabled() {
        return true;
    }

    @Test
    void run() throws Exception {
        var execution = triggerFlow(this.getClass().getClassLoader(), "flows","db2-listen");

        var rows = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");
        assertThat(rows.size(), is(1));
    }

    @Override
    protected String getUrl() {
        return "jdbc:db2://localhost:5023/testdb";
    }

    @Override
    protected String getUsername() {
        return "db2inst1";
    }

    @Override
    protected String getPassword() {
        return "password";
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/db2.sql");
    }
}