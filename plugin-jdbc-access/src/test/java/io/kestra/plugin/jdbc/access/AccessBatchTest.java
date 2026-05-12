package io.kestra.plugin.jdbc.access;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;

import static io.kestra.core.models.tasks.common.FetchType.FETCH;
import static io.kestra.core.models.tasks.common.FetchType.STORE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class AccessBatchTest extends AbstractRdbmsTest {

    private String currentDbPath;
    private String currentUrl;

    @Test
    void batchInsert() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        // Fetch rows to store
        Query query = Query.builder()
            .url(Property.ofValue(getUrl()))
            .fetchType(Property.ofValue(STORE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("SELECT id, text_column, int_column FROM batch_source"))
            .build();

        AbstractJdbcQuery.Output queryOutput = query.run(runContext);
        assertThat(queryOutput.getUri(), notNullValue());

        // Batch insert into destination table
        Batch batch = Batch.builder()
            .from(Property.ofValue(queryOutput.getUri().toString()))
            .url(Property.ofValue(getUrl()))
            .sql(Property.ofValue("INSERT INTO batch_dest (id, text_column, int_column) VALUES (?, ?, ?)"))
            .build();

        AbstractJdbcBatch.Output batchOutput = batch.run(runContext);
        assertThat(batchOutput.getRowCount(), is(3L));

        // Verify inserts
        Query verify = Query.builder()
            .url(Property.ofValue(getUrl()))
            .fetchType(Property.ofValue(FETCH))
            .sql(Property.ofValue("SELECT * FROM batch_dest ORDER BY id"))
            .build();

        AbstractJdbcQuery.Output verifyOutput = verify.run(runContext);
        assertThat(verifyOutput.getRows(), hasSize(3));
        assertThat(verifyOutput.getRows().getFirst().get("text_column"), is("Row 1"));
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
        currentDbPath = System.getProperty("java.io.tmpdir") + "/test_access_batch_" + UUID.randomUUID() + ".accdb";
        currentUrl = "jdbc:ucanaccess://" + currentDbPath;
        try {
            TestUtils.createDatabase(currentDbPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try (var conn = DriverManager.getConnection(currentUrl);
             var stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE batch_source (
                    id INTEGER,
                    text_column VARCHAR(255),
                    int_column INTEGER
                )
                """);
            stmt.execute("INSERT INTO batch_source (id, text_column, int_column) VALUES (1, 'Row 1', 100)");
            stmt.execute("INSERT INTO batch_source (id, text_column, int_column) VALUES (2, 'Row 2', 200)");
            stmt.execute("INSERT INTO batch_source (id, text_column, int_column) VALUES (3, 'Row 3', 300)");

            stmt.execute("""
                CREATE TABLE batch_dest (
                    id INTEGER,
                    text_column VARCHAR(255),
                    int_column INTEGER
                )
                """);
        }
    }
}
