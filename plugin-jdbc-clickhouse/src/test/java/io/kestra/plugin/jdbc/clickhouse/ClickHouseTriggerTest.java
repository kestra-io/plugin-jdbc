package io.kestra.plugin.jdbc.clickhouse;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.plugin.jdbc.AbstractJdbcTriggerTest;
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
class ClickHouseTriggerTest extends AbstractJdbcTriggerTest {

    @Test
    void run() throws Exception {
        var execution = triggerFlow(this.getClass().getClassLoader(), "flows","clickhouse-listen");

        var rows = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");
        assertThat(rows.size(), is(1));
    }

    @Test
    void afterSQLRunsAfterFetch() throws Exception {
        try (Connection connection = getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS trigger_after_sql");
            statement.execute("CREATE TABLE trigger_after_sql (id Int32) ENGINE = Memory");
            statement.execute("INSERT INTO trigger_after_sql VALUES (1)");
        }

        var trigger = Trigger.builder()
            .id("watch")
            .type(Trigger.class.getName())
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .sql(Property.ofValue("SELECT id FROM trigger_after_sql"))
            .afterSQL(Property.ofValue("TRUNCATE TABLE trigger_after_sql"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        assertThat(trigger.runQuery(runContextFactory.of()).getSize(), is(1L));
        // afterSQL emptied the table, so the next poll finds nothing
        assertThat(trigger.runQuery(runContextFactory.of()).getSize(), is(0L));
    }

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
        executeSqlScript("scripts/clickhouse.sql");
    }
}