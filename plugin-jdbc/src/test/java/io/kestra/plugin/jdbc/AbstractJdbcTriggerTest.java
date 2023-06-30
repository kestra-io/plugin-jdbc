package io.kestra.plugin.jdbc;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.Worker;
import io.kestra.core.schedulers.AbstractScheduler;
import io.kestra.core.schedulers.DefaultScheduler;
import io.kestra.core.schedulers.SchedulerTriggerStateInterface;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
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

public abstract class AbstractJdbcTriggerTest {
    @Inject
    protected ApplicationContext applicationContext;

    @Inject
    protected SchedulerTriggerStateInterface triggerState;

    @Inject
    protected FlowListeners flowListenersService;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    protected QueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    protected Execution triggerFlow(ClassLoader classLoader, String flowRepository, String flow) throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        Worker worker = new Worker(applicationContext, 8, null);
        try (
            AbstractScheduler scheduler = new DefaultScheduler(
                this.applicationContext,
                this.flowListenersService,
                this.triggerState
            );
        ) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            executionQueue.receive(execution -> {
                last.set(execution);

                queueCount.countDown();
                assertThat(execution.getFlowId(), is(flow));
            });

            worker.run();
            scheduler.run();
            repositoryLoader.load(Objects.requireNonNull(classLoader.getResource(flowRepository)));

            queueCount.await(1, TimeUnit.MINUTES);

            return last.get();
        }
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
