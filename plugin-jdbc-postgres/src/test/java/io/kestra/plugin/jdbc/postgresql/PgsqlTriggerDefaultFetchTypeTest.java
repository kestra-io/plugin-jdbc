package io.kestra.plugin.jdbc.postgresql;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.plugin.jdbc.AbstractJdbcTriggerTest;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@KestraTest
class PgsqlTriggerDefaultFetchTypeTest extends AbstractJdbcTriggerTest {

    @Test
    void firesWithoutExplicitFetchType() throws Exception {
        var execution = triggerFlow(this.getClass().getClassLoader(), "flows", "pgsql-listen-default-fetchtype");

        var rows = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");
        assertThat(rows.size(), greaterThanOrEqualTo(1));
    }

    @Override
    protected String getUrl() {
        return "jdbc:postgresql://127.0.0.1:56982/";
    }

    @Override
    protected String getUsername() {
        return "postgres";
    }

    @Override
    protected String getPassword() {
        return "pg_passwd";
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/postgres.sql");
    }
}
