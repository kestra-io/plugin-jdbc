package io.kestra.plugin.jdbc.sqlite;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.tenant.TenantService;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

import static io.kestra.core.models.tasks.common.FetchType.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class SqliteQueriesTest extends AbstractRdbmsTest {

    @Test
    void testMultiSelectWithParameters() throws Exception {
        RunContext runContext = runContextFactory.of(Collections.emptyMap());

        Map<String, Object> parameters = Map.of(
            "age", 40,
            "brand", "Apple",
            "cpu_frequency", 1.5
        );

        Queries taskGet = Queries.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .fetchType(Property.ofValue(FETCH))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("""
                SELECT firstName, lastName, age FROM employee where age > :age and age < :age + 10;
                SELECT brand, model FROM laptop where brand = :brand and cpu_frequency > :cpu_frequency;
                """))
            .parameters(Property.ofValue(parameters))
            .build();

        AbstractJdbcQueries.MultiQueryOutput runOutput = taskGet.run(runContext);
        assertThat(runOutput.getOutputs().size(), is(2));

        List<Map<String, Object>> employees = runOutput.getOutputs().getFirst().getRows();
        assertThat("employees", employees, notNullValue());
        assertThat("employees", employees.size(), is(1));
        assertThat("employee selected", employees.getFirst().get("age"), is(45));
        assertThat("employee selected", employees.getFirst().get("firstName"), is("John"));
        assertThat("employee selected", employees.getFirst().get("lastName"), is("Doe"));

        List<Map<String, Object>>laptops = runOutput.getOutputs().getLast().getRows();
        assertThat("laptops", laptops, notNullValue());
        assertThat("laptops", laptops.size(), is(1));
        assertThat("selected laptop", laptops.getFirst().get("brand"), is("Apple"));
    }

    @Test
    void testRollback() throws Exception {
        RunContext runContext = runContextFactory.of(Collections.emptyMap());

        //Queries should pass in a transaction
        Queries queriesPass = Queries.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .fetchType(Property.ofValue(FETCH_ONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("""
                DROP TABLE IF EXISTS test_transaction;
                CREATE TABLE test_transaction(id INTEGER PRIMARY KEY);
                INSERT INTO test_transaction (id) VALUES (1);
                SELECT COUNT(id) as transaction_count FROM test_transaction;
                """))
            .build();

        AbstractJdbcQueries.MultiQueryOutput runOutput = queriesPass.run(runContext);
        assertThat(runOutput.getOutputs().size(), is(1));
        assertThat(runOutput.getOutputs().getFirst().getRow().get("transaction_count"), is(1));

        //Queries should fail due to bad sql
        Queries insertsFail = Queries.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .fetchType(Property.ofValue(FETCH_ONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("""
                INSERT INTO test_transaction (id) VALUES (2);
                INSERT INTO test_transaction (id) VALUES (3f);
                """))//Try inserting before failing
            .build();

        assertThrows(Exception.class, () -> insertsFail.run(runContext));

        //Final query to verify the amount of updated rows
        Queries verifyQuery = Queries.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .fetchType(Property.ofValue(FETCH_ONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("""
                SELECT COUNT(id) as transaction_count FROM test_transaction;
                """)) //Try inserting before failing
            .build();

        AbstractJdbcQueries.MultiQueryOutput verifyOutput = verifyQuery.run(runContext);
        assertThat(verifyOutput.getOutputs().size(), is(1));
        assertThat(verifyOutput.getOutputs().getFirst().getRow().get("transaction_count"), is(1));
    }

    @Test
    void testNonTransactionalShouldNotRollback() throws Exception {
        RunContext runContext = runContextFactory.of(Collections.emptyMap());

        //Queries should pass in a transaction
        Queries insertOneAndFail = Queries.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .fetchType(Property.ofValue(FETCH_ONE))
            .transaction(Property.ofValue(false))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("""
                DROP TABLE IF EXISTS test_transaction;
                CREATE TABLE test_transaction(id INTEGER PRIMARY KEY);
                INSERT INTO test_transaction (id) VALUES (1);
                INSERT INTO test_transaction (id) VALUES (1f);
                INSERT INTO test_transaction (id) VALUES (2);
                """))
            .build();

        assertThrows(Exception.class, () -> insertOneAndFail.run(runContext));

        //Final query to verify the amount of updated rows
        Queries verifyQuery = Queries.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .fetchType(Property.ofValue(FETCH_ONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("""
                SELECT COUNT(id) as transaction_count FROM test_transaction;
                """)) //Try inserting before failing
            .build();

        AbstractJdbcQueries.MultiQueryOutput verifyOutput = verifyQuery.run(runContext);
        assertThat(verifyOutput.getOutputs().size(), is(1));
        assertThat(verifyOutput.getOutputs().getFirst().getRow().get("transaction_count"), is(1));
    }

    @Test
    void selectFromExistingDatabaseAndOutputDatabase() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        URL resource = SqliteTest.class.getClassLoader().getResource("db/Chinook_Sqlite.sqlite");

        URI input = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/file/storage/get.yml"),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );

        //Fetch and insert data, output dbFile, check size of outputs
        Queries task = Queries.builder()
            .url(Property.ofValue("jdbc:sqlite:Chinook_Sqlite.sqlite"))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .fetchType(Property.ofValue(FETCH))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sqliteFile(Property.ofValue(input.toString()))
            .outputDbFile(Property.ofValue(true))
            .sql(Property.ofValue("""
                SELECT * FROM Genre;
                INSERT INTO Genre (GenreId, Name) VALUES (26, 'TestInsert');
                """))
            .build();

        Queries.Output runOutput = task.run(runContext);

        assertThat(runOutput.getOutputs().size(), is(1));
        assertThat(runOutput.getOutputs().getFirst(), notNullValue());
        assertThat(runOutput.getOutputs().getFirst().getRows().size(), is(25));

        //Check DB size
        //Update DB and output file
        Queries check = Queries.builder()
            .url(Property.ofValue("jdbc:sqlite:Chinook_Sqlite.sqlite"))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .fetchType(Property.ofValue(FETCH))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sqliteFile(Property.ofValue(runOutput.getDatabaseUri().toString()))
            .sql(Property.ofValue("""
                SELECT * FROM Genre;
                """))
            .build();
        Queries.Output checkOutput = check.run(runContext);
        assertThat(checkOutput.getOutputs().getFirst().getRows(), notNullValue());
        assertThat(checkOutput.getOutputs().getFirst().getRows().size(), is(26));
        assertThat(checkOutput.getOutputs().getFirst().getRows().stream().anyMatch(row -> row.get("Name").equals("TestInsert")), is(true));
    }

    @Override
    protected String getUrl() {
        return TestUtils.url();
    }

    @Override
    protected String getUsername() {
        return TestUtils.username();
    }

    @Override
    protected String getPassword() {
        return TestUtils.password();
    }

    protected Connection getConnection() throws SQLException {
        Properties props = new Properties();
        props.put("jdbc.url", getUrl());
        props.put("user", getUsername());
        props.put("password", getPassword());

        return DriverManager.getConnection(props.getProperty("jdbc.url"), props);
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/sqlite_queries.sql");
    }
}
