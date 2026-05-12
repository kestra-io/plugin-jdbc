package io.kestra.plugin.jdbc.access;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;

import static io.kestra.core.models.tasks.common.FetchType.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class AccessTest extends AbstractRdbmsTest {

    // Unique per test run to avoid UCanAccess in-memory cache collisions across @BeforeEach calls
    private String currentDbPath;
    private String currentUrl;

    @Test
    void select() throws Exception {
        var runContext = runContextFactory.of(ImmutableMap.of());

        var task = Query.builder()
            .url(Property.ofValue(getUrl()))
            .fetchType(Property.ofValue(FETCH_ONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("SELECT * FROM access_types"))
            .build();

        var runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());
        assertThat(runOutput.getRow().get("id"), is(1));
        assertThat(runOutput.getRow().get("text_column"), is("Sample Text"));
        assertThat(runOutput.getRow().get("int_column"), is(42));
        assertThat(runOutput.getRow().get("d"), nullValue());
    }

    @Test
    void selectMultipleRows() throws Exception {
        var runContext = runContextFactory.of(ImmutableMap.of());

        var task = Query.builder()
            .url(Property.ofValue(getUrl()))
            .fetchType(Property.ofValue(FETCH))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("SELECT * FROM access_types"))
            .build();

        var runOutput = task.run(runContext);
        assertThat(runOutput.getRows(), notNullValue());
        assertThat(runOutput.getRows(), not(empty()));
    }

    @Test
    void update() throws Exception {
        var runContext = runContextFactory.of(ImmutableMap.of());

        Query taskUpdate = Query.builder()
            .url(Property.ofValue(getUrl()))
            .fetchType(Property.ofValue(FETCH_ONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("UPDATE access_types SET d = 'D'"))
            .build();
        taskUpdate.run(runContext);

        Query taskGet = Query.builder()
            .url(Property.ofValue(getUrl()))
            .fetchType(Property.ofValue(FETCH_ONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("SELECT d FROM access_types"))
            .build();

        AbstractJdbcQuery.Output runOutput = taskGet.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());
        assertThat(runOutput.getRow().get("d"), is("D"));
    }

    @Test
    void outputDbFileTrueWithoutAccessFile_shouldCreateAndOutputDatabase() throws Exception {
        var runContext = runContextFactory.of(ImmutableMap.of());
        var dbName = "access-roundtrip-" + UUID.randomUUID() + ".accdb";

        // Create table in a new DB file (UCanAccess auto-creates via newDatabaseVersion)
        Query create = Query.builder()
            .url(Property.ofValue("jdbc:ucanaccess:///" + dbName))
            .fetchType(Property.ofValue(NONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .outputDbFile(Property.ofValue(true))
            .sql(Property.ofValue("CREATE TABLE t (id INTEGER, name VARCHAR(255))"))
            .build();

        Query.Output createOutput = create.run(runContext);
        assertThat(createOutput.getDatabaseUri(), notNullValue());

        // Insert row using stored DB
        Query insert = Query.builder()
            .url(Property.ofValue("jdbc:ucanaccess:///" + dbName))
            .fetchType(Property.ofValue(NONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .accessFile(Property.ofValue(createOutput.getDatabaseUri().toString()))
            .outputDbFile(Property.ofValue(true))
            .sql(Property.ofValue("INSERT INTO t (id, name) VALUES (1, 'hello')"))
            .build();

        Query.Output insertOutput = insert.run(runContext);
        assertThat(insertOutput.getDatabaseUri(), notNullValue());

        // Read back
        Query readBack = Query.builder()
            .url(Property.ofValue("jdbc:ucanaccess:///" + dbName))
            .fetchType(Property.ofValue(FETCH_ONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .accessFile(Property.ofValue(insertOutput.getDatabaseUri().toString()))
            .sql(Property.ofValue("SELECT name FROM t WHERE id = 1"))
            .build();

        AbstractJdbcQuery.Output readOutput = readBack.run(runContext);
        assertThat(readOutput.getRow(), notNullValue());
        assertThat(readOutput.getRow().get("name"), is("hello"));
    }

    @Override
    protected String getUrl() {
        return currentUrl;
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
    protected Connection getConnection() throws SQLException {
        return DriverManager.getConnection(getUrl());
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        // Use a unique path each invocation to avoid UCanAccess in-memory cache collisions
        currentDbPath = System.getProperty("java.io.tmpdir") + "/test_access_query_" + UUID.randomUUID() + ".accdb";
        currentUrl = "jdbc:ucanaccess://" + currentDbPath;
        try {
            TestUtils.createDatabase(currentDbPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try (var conn = DriverManager.getConnection(currentUrl)) {
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
