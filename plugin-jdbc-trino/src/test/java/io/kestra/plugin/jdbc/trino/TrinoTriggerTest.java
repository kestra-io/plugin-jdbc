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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
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

    @Test
    void propertiesAreForwardedToTheQuery() throws Exception {
        try (Connection connection = getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS trigger_events");
            statement.execute("CREATE TABLE trigger_events (id INTEGER)");
            statement.execute("INSERT INTO trigger_events VALUES (3), (4)");
        }

        // The typed properties of the trigger reach the query it runs.
        var trigger = Trigger.builder()
            .id("watch")
            .type(Trigger.class.getName())
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .source(Property.ofValue("kestra_trigger_test"))
            .clientTags(Property.ofValue(List.of("kestra", "trigger")))
            .sql(Property.ofValue("SELECT id FROM trigger_events WHERE id = 4"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        var output = trigger.runQuery(runContextFactory.of());
        assertThat(output.getSize(), is(1L));
        assertThat(output.getRows().getFirst().get("id"), is(4));
        assertThat(output.getQueryId(), notNullValue());

        try (Connection connection = getConnection(); PreparedStatement statement = connection.prepareStatement("""
            SELECT source
            FROM system.runtime.queries
            WHERE query_id = ?
            """)) {
            statement.setString(1, output.getQueryId());
            try (var resultSet = statement.executeQuery()) {
                assertThat(resultSet.next(), is(true));
                assertThat(resultSet.getString("source"), is("kestra_trigger_test"));
            }
        }
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