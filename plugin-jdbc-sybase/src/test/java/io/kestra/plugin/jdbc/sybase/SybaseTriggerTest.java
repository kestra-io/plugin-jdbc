package io.kestra.plugin.jdbc.sybase;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.plugin.jdbc.AbstractJdbcTriggerTest;
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
@Disabled("Lauching Sybase via our docker-compose crash the CI")
class SybaseTriggerTest extends AbstractJdbcTriggerTest {

    @Test
    void run() throws Exception {
        var execution = triggerFlow(this.getClass().getClassLoader(), "flows","sybase-listen");

        var rows = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");
        assertThat(rows.size(), is(1));
    }

    @Override
    protected String getUrl() {
        return "jdbc:sybase:Tds:127.0.0.1:5000/kestra";
    }

    @Override
    protected String getUsername() {
        return "sa";
    }

    @Override
    protected String getPassword() {
        return "myPassword";
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/sybase.sql");
    }
}