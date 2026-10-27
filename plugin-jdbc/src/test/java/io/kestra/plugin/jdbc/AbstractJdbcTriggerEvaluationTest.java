package io.kestra.plugin.jdbc;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.net.URI;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Runs each concrete trigger and its Query against an in-memory JDBC connection. Only the
 * driver boundary is replaced: no warehouse, credentials, Docker, runner or scheduler is needed.
 * Vendor SQL dialects, wire protocols and TLS still belong in integration tests.
 */
public abstract class AbstractJdbcTriggerEvaluationTest {
    @Inject
    protected RunContextFactory runContextFactory;

    private TestDriver driver;
    private Connection database;

    protected abstract AbstractJdbcTrigger.AbstractJdbcTriggerBuilder<?, ?> triggerBuilder();

    @BeforeEach
    protected void createDatabase() throws SQLException {
        var scheme = triggerBuilder().build().getScheme();
        var id = UUID.randomUUID().toString();
        var h2Url = "jdbc:h2:mem:" + id + ";DATABASE_TO_LOWER=TRUE";
        database = DriverManager.getConnection(h2Url);
        // Match the plugin's scheme validation, but never a real vendor driver's URL syntax.
        driver = new TestDriver(scheme + "-trigger-test:" + id, h2Url);
        DriverManager.registerDriver(driver);
        try (var statement = database.createStatement()) {
            statement.execute("CREATE TABLE trigger_items (id INTEGER PRIMARY KEY, name VARCHAR(32))");
            statement.execute("INSERT INTO trigger_items VALUES (1, 'first'), (2, 'second')");
        }
    }

    @AfterEach
    protected void closeDatabase() throws SQLException {
        try {
            if (driver != null) {
                DriverManager.deregisterDriver(driver);
            }
        } finally {
            if (database != null) {
                database.close();
            }
        }
    }

    protected AbstractJdbcTrigger.AbstractJdbcTriggerBuilder<?, ?> configuredTrigger() {
        return triggerBuilder()
            .id("watch")
            .url(Property.ofValue(driver.url))
            .username(Property.ofValue("test-user"))
            .password(Property.ofValue("test-password"))
            .sql(Property.ofValue("SELECT id, name FROM trigger_items ORDER BY id"));
    }

    protected Optional<Execution> evaluate(AbstractJdbcTrigger trigger) throws Exception {
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        return trigger.evaluate(context.getKey(), context.getValue().context());
    }

    @ParameterizedTest
    @EnumSource(value = FetchType.class, names = {"FETCH", "FETCH_ONE", "STORE"})
    protected void noRowsDoesNotTrigger(FetchType fetchType) throws Exception {
        var trigger = configuredTrigger()
            .sql(Property.ofValue("SELECT id FROM trigger_items WHERE id = 0"))
            .fetchType(Property.ofValue(fetchType))
            .build();

        assertThat(evaluate(trigger).isEmpty(), is(true));
    }

    @Test
    protected void defaultFetchExposesAllRows() throws Exception {
        var execution = evaluate(configuredTrigger().build()).orElseThrow();

        assertThat(execution.getTrigger().getId(), is("watch"));
        assertThat(((Number) execution.getTrigger().getVariables().get("size")).longValue(), is(2L));
        assertThat(execution.getTrigger().getVariables().get("rows"), is(expectedRows()));
    }

    @Test
    protected void fetchOneExposesFirstRow() throws Exception {
        var trigger = configuredTrigger().fetchType(Property.ofValue(FetchType.FETCH_ONE)).build();

        var variables = evaluate(trigger).orElseThrow().getTrigger().getVariables();

        assertThat(((Number) variables.get("size")).longValue(), is(1L));
        assertThat(variables.get("row"), is(expectedRows().getFirst()));
    }

    @Test
    protected void storeExposesReadableRows() throws Exception {
        var trigger = configuredTrigger().fetchType(Property.ofValue(FetchType.STORE)).build();
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        var variables = trigger.evaluate(context.getKey(), context.getValue().context())
            .orElseThrow().getTrigger().getVariables();

        assertThat(((Number) variables.get("size")).longValue(), is(2L));
        var uri = URI.create(variables.get("uri").toString());
        try (var input = context.getKey().getRunContext().storage().getFile(uri)) {
            var rows = FileSerde.readAll(input).collectList().block();
            assertThat(rows, is(expectedRows()));
        }
    }

    private List<Map<String, Object>> expectedRows() {
        return List.of(Map.of("id", 1, "name", "first"), Map.of("id", 2, "name", "second"));
    }

    private static class TestDriver implements Driver {
        private final String url;
        private final String h2Url;

        private TestDriver(String url, String h2Url) {
            this.url = url;
            this.h2Url = h2Url;
        }

        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            if (!acceptsURL(url)) {
                return null;
            }
            return DriverManager.getConnection(h2Url);
        }

        @Override
        public boolean acceptsURL(String url) {
            return this.url.equals(url);
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }
    }
}
