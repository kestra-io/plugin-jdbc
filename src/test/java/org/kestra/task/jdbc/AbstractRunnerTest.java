package org.kestra.task.jdbc;

import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.BeforeEach;
import org.kestra.core.repositories.LocalFlowRepositoryLoader;
import org.kestra.core.runners.RunnerUtils;
import org.kestra.core.storages.StorageInterface;
import org.kestra.runner.memory.MemoryRunner;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

@MicronautTest
public class AbstractRunnerTest {
    @Inject
    protected MemoryRunner runner;

    @Inject
    protected RunnerUtils runnerUtils;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Inject
    StorageInterface storageInterface;

    @BeforeEach
    private void init() throws IOException, URISyntaxException, SQLException {
        loadH2Driver();

        repositoryLoader.load(Objects.requireNonNull(JdbcQueryRunnerTest.class.getClassLoader().getResource("flows")));
        if (!this.runner.isRunning()) {
            this.runner.run();
        }

        // Load sample data into H2
        loadSampleDataIntoH2();
    }

    private void loadSampleDataIntoH2() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:h2:mem:default", "sa", "sa");
        Statement stmt = conn.createStatement();

        stmt.execute("drop table if exists csvdata");
        stmt.execute("create table csvdata as SELECT * from CSVREAD( 'classpath:source/full.csv') ");

        stmt.close();
    }

    private void loadH2Driver() throws SQLException {
        // No need to manually load the driver thanks to Java Service Provider mechanism
        // Driver h2Driver = new org.h2.Driver();
        // DriverManager.registerDriver(h2Driver);
    }
}
