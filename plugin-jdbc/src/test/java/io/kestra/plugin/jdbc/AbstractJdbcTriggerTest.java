package io.kestra.plugin.jdbc;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.DispatchQueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
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
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public abstract class AbstractJdbcTriggerTest {
    @Inject
    protected DispatchQueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    protected Execution triggerFlow(ClassLoader classLoader, String flowRepository, String flow) throws Exception {
        CountDownLatch queueCount = new CountDownLatch(1);
        AtomicReference<Execution> last = new AtomicReference<>();

        executionQueue.addListener(execution -> {
            if (execution.getFlowId().equals(flow)) {
                last.set(execution);
                queueCount.countDown();
            }
        });

        repositoryLoader.load(Objects.requireNonNull(classLoader.getResource(flowRepository)));

        boolean await = queueCount.await(1, TimeUnit.MINUTES);
        assertThat(await, is(true));

        Execution execution = last.get();
        assertThat(execution, notNullValue());
        return execution;
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
