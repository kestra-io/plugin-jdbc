package io.kestra.plugin.jdbc.access;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.plugin.jdbc.AbstractJdbcTriggerTest;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class AccessTriggerTest extends AbstractJdbcTriggerTest {

    private static final String DB_PATH = System.getProperty("java.io.tmpdir") + "/test_access_trigger.accdb";
    private static final String URL = "jdbc:ucanaccess://" + DB_PATH;

    @Test
    void run() throws Exception {
        var execution = triggerFlow(this.getClass().getClassLoader(), "flows", "access-listen");

        var rows = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");
        assertThat(rows.size(), is(1));
    }

    @Override
    protected String getUrl() {
        return URL;
    }

    @Override
    protected String getUsername() {
        return TestUtils.username();
    }

    @Override
    protected String getPassword() {
        return TestUtils.password();
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        try {
            TestUtils.createDatabase(DB_PATH);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try (var conn = DriverManager.getConnection(getUrl())) {
            try (var stmt = conn.createStatement()) {
                stmt.execute("""
                    CREATE TABLE access_types (
                        id INTEGER,
                        text_column VARCHAR(255),
                        int_column INTEGER,
                        d VARCHAR(255)
                    )
                    """);
                stmt.execute("INSERT INTO access_types (id, text_column, int_column, d) VALUES (1, 'Sample Text', 42, NULL)");
            }
        }
    }
}
