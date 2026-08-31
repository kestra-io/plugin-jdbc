package io.kestra.plugin.jdbc;

import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.scheduler.model.TriggerState;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public abstract class AbstractJdbcTriggerTest {
    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Inject
    protected RunContextFactory runContextFactory;

    /**
     * Loads the flow fixture and evaluates its trigger directly, rather than waiting for the
     * scheduler to fire it. Driving the scheduler from a plugin test is unreliable and only
     * surfaces as a bare latch timeout when it does not fire.
     */
    protected Execution triggerFlow(ClassLoader classLoader, String flowRepository, String flow) throws Exception {
        List<FlowWithSource> loaded = repositoryLoader.load(Objects.requireNonNull(classLoader.getResource(flowRepository)));

        FlowWithSource target = loaded.stream()
            .filter(candidate -> candidate.getId().equals(flow))
            .findFirst()
            .orElseThrow(() -> new AssertionError("flow " + flow + " was not loaded from " + flowRepository));

        AbstractTrigger abstractTrigger = target.getTriggers().getFirst();

        Map.Entry<ConditionContext, TriggerState> context = TestsUtils.mockTrigger(runContextFactory, abstractTrigger);
        Optional<Execution> execution = ((PollingTriggerInterface) abstractTrigger)
            .evaluate(context.getKey(), context.getValue().context());

        assertThat(execution.isPresent(), is(true));
        return execution.get();
    }

    protected abstract String getUrl();

    protected String getUsername() {
        return null;
    }

    protected String getPassword() {
        return null;
    }

    protected abstract void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException;

    protected Connection getConnection() throws SQLException {
        return DriverManager.getConnection(getUrl(), getUsername(), getPassword());
    }

    protected void executeSqlScript(String path) throws SQLException, URISyntaxException, FileNotFoundException {
        URL url = Objects.requireNonNull(AbstractJdbcTriggerTest.class.getClassLoader().getResource(path));
        FileReader fileReader = new FileReader(new File(url.toURI()));
        RunScript.execute(getConnection(), fileReader);
    }

    @BeforeEach
    public void init() throws IOException, URISyntaxException, SQLException {
        initDatabase();
    }
}
