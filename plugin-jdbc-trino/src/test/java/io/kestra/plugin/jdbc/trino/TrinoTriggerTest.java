package io.kestra.plugin.jdbc.trino;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.plugin.jdbc.AbstractJdbcTriggerTest;
import io.kestra.core.junit.annotations.KestraTest;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest(startRunner = true, startScheduler = true)
class TrinoTriggerTest extends AbstractJdbcTriggerTest {

    @Test
    void run() throws Exception {
        var execution = triggerFlow(this.getClass().getClassLoader(), "flows","trino-listen");

        var rows = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");
        assertThat(rows.size(), is(1));
    }

    @Test
    void parametersReachTheQuery() throws Exception {
        try (Connection connection = getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS trigger_events");
            statement.execute("CREATE TABLE trigger_events (id INTEGER)");
            statement.execute("INSERT INTO trigger_events VALUES (1), (2)");
        }

        var trigger = Trigger.builder()
            .id("watch")
            .type(Trigger.class.getName())
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .sql(Property.ofValue("SELECT id FROM trigger_events WHERE id = :id"))
            .parameters(Property.ofValue(Map.of("id", 2)))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        var output = trigger.runQuery(runContextFactory.of());
        assertThat(output.getSize(), is(1L));
        assertThat(output.getRows().getFirst().get("id"), is(2));
    }

    @Test
    void afterSQLReachesTheQuery() {
        // The memory catalog only accepts autocommit writes, so a failing read is used to show afterSQL runs
        var trigger = Trigger.builder()
            .id("watch")
            .type(Trigger.class.getName())
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .sql(Property.ofValue("SELECT 1 AS id"))
            .afterSQL(Property.ofValue("SELECT * FROM trigger_after_sql_missing"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        var exception = assertThrows(SQLException.class, () -> trigger.runQuery(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("trigger_after_sql_missing"));
    }

    @Override
    protected String getUrl() {
        return "jdbc:trino://localhost:48080/memory/default";
    }

    @Override
    protected String getUsername() {
        return "fake";
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/trino.sql");
    }
}