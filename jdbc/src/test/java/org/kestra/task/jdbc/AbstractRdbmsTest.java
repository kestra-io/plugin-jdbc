package org.kestra.task.jdbc;

import org.h2.tools.RunScript;
import org.junit.jupiter.api.BeforeEach;
import org.kestra.core.repositories.LocalFlowRepositoryLoader;
import org.kestra.core.runners.RunContextFactory;
import org.kestra.core.runners.RunnerUtils;
import org.kestra.core.storages.StorageInterface;
import org.kestra.runner.memory.MemoryRunner;

import javax.inject.Inject;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

public abstract class AbstractRdbmsTest {
    @Inject
    protected RunContextFactory runContextFactory;

    @Inject
    protected MemoryRunner runner;

    @Inject
    protected RunnerUtils runnerUtils;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Inject
    protected StorageInterface storageInterface;

    protected abstract String getUrl();

    protected abstract String getUsername();

    protected abstract String getPassword();

    protected abstract void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException;

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(getUrl(), getUsername(), getPassword());
    }

    protected void executeQuery(String query) throws SQLException {
        Statement stmt = getConnection().createStatement();
        stmt.execute(query);
        stmt.close();
    }

    protected void executeSqlScript(String path) throws SQLException, URISyntaxException, FileNotFoundException {
        URL url = Objects.requireNonNull(AbstractRdbmsTest.class.getClassLoader().getResource(path));
        FileReader fileReader = new FileReader(new File(url.toURI()));
        RunScript.execute(getConnection(), fileReader);
    }

    @BeforeEach
    public void init() throws IOException, URISyntaxException, SQLException {
        // We force the timezone to UTC to have consistent tests
        // TimeZone.setDefault(TimeZone.getTimeZone("Europe/Paris"));

        repositoryLoader.load(Objects.requireNonNull(AbstractRdbmsTest.class.getClassLoader().getResource("flows")));
        if (!this.runner.isRunning()) {
            this.runner.run();
        }
        initDatabase();
    }
}
