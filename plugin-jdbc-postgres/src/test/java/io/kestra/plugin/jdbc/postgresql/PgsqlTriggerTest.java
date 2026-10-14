package io.kestra.plugin.jdbc.postgresql;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.plugin.jdbc.AbstractJdbcTriggerTest;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest(startRunner = true, startScheduler = true)
class PgsqlTriggerTest extends AbstractJdbcTriggerTest {

    @Test
    void run() throws Exception {
        var execution = triggerFlow(this.getClass().getClassLoader(), "flows","pgsql-listen");

        var rows = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");
        assertThat(rows.size(), is(2));
    }

    @Test
    void sslSettingsReachTheQuery() {
        // This server has no TLS, so the connection can only fail if sslMode reaches the driver
        var trigger = Trigger.builder()
            .id("watch")
            .type(Trigger.class.getName())
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .sslMode(Property.ofValue(PostgresConnectionInterface.SslMode.REQUIRE))
            .sql(Property.ofValue("SELECT 1"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();

        var exception = assertThrows(Exception.class, () -> trigger.runQuery(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("SSL"));
    }

    @Test
    void connectionPoolingDisabledOpensANewConnectionEachPoll() throws Exception {
        var trigger = Trigger.builder()
            .id("watch")
            .type(Trigger.class.getName())
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .connectionPooling(Property.ofValue(false))
            .sql(Property.ofValue("SELECT pg_backend_pid() AS pid"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();

        var firstPid = trigger.runQuery(runContextFactory.of()).getRow().get("pid");
        var secondPid = trigger.runQuery(runContextFactory.of()).getRow().get("pid");

        // A pooled connection would be handed back to the second poll and report the same backend
        assertThat(secondPid, not(is(firstPid)));
    }

    @Override
    protected String getUrl() {
        return TestUtils.url();
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