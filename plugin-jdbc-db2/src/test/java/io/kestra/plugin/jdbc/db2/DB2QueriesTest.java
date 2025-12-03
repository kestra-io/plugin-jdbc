package io.kestra.plugin.jdbc.db2;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.kestra.core.models.tasks.common.FetchType.FETCH;
import static io.kestra.core.models.tasks.common.FetchType.FETCH_ONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@Disabled("Disabled for CI")
public class DB2QueriesTest extends AbstractRdbmsTest {

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
                SELECT * FROM employee;
                """))
            .parameters(Property.ofValue(parameters))
            .build();

        AbstractJdbcQueries.MultiQueryOutput runOutput = taskGet.run(runContext);
        assertThat(runOutput.getOutputs().size(), is(3));

        List<Map<String, Object>> employees = runOutput.getOutputs().getFirst().getRows();
        assertThat("employees", employees, notNullValue());
        assertThat("employees", employees.size(), is(1));
        assertThat("employee selected", employees.getFirst().get("AGE"), is(45));
        assertThat("employee selected", employees.getFirst().get("FIRSTNAME"), is("John"));
        assertThat("employee selected", employees.getFirst().get("LASTNAME"), is("Doe"));

        List<Map<String, Object>> laptops = runOutput.getOutputs().get(1).getRows();
        assertThat("laptops", laptops, notNullValue());
        assertThat("laptops", laptops.size(), is(1));
        assertThat("selected laptop", laptops.getFirst().get("BRAND"), is("Apple"));

        List<Map<String, Object>> allEmployees = runOutput.getOutputs().getLast().getRows();
        assertThat("All employees", allEmployees, notNullValue());
        assertThat("All employees size", allEmployees.size(), is(4));
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
                DROP TABLE IF EXISTS DB2INST1.test_transaction;
                CREATE TABLE DB2INST1.test_transaction(id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name VARCHAR(230));
                INSERT INTO DB2INST1.test_transaction (name) VALUES ('test_insert_1');
                SELECT COUNT(id) as TRANSACTION_COUNT FROM DB2INST1.test_transaction;
                """))
            .build();

        AbstractJdbcQueries.MultiQueryOutput runOutput = queriesPass.run(runContext);
        assertThat(runOutput.getOutputs().size(), is(1));
        assertThat(runOutput.getOutputs().getFirst().getRow().get("TRANSACTION_COUNT"), is(1));

        //Queries should fail due to bad sql
        Queries insertsFail = Queries.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .fetchType(Property.ofValue(FETCH_ONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("""
                INSERT INTO DB2INST1.test_transaction (name) VALUES ('test_insert_2');
                INSERT INTO DB2INST1.test_transaction (name) VALUES (3f);
                """)) //Try inserting before failing
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
                SELECT COUNT(id) as TRANSACTION_COUNT FROM DB2INST1.test_transaction;
                """)) //Try inserting before failing
            .build();

        AbstractJdbcQueries.MultiQueryOutput verifyOutput = verifyQuery.run(runContext);
        assertThat(verifyOutput.getOutputs().size(), is(1));
        assertThat(verifyOutput.getOutputs().getFirst().getRow().get("TRANSACTION_COUNT"), is(1));
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
                DROP TABLE IF EXISTS DB2INST1.test_transaction;
                CREATE TABLE DB2INST1.test_transaction(id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name VARCHAR(230));
                INSERT INTO DB2INST1.test_transaction (name) VALUES ('test_insert_1');
                INSERT INTO DB2INST1.test_transaction (id) VALUES (1f);
                INSERT INTO DB2INST1.test_transaction (id) VALUES ('test_insert_2);
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
                SELECT COUNT(id) as TRANSACTION_COUNT FROM DB2INST1.test_transaction;
                """)) //Try inserting before failing
            .build();

        AbstractJdbcQueries.MultiQueryOutput verifyOutput = verifyQuery.run(runContext);
        assertThat(verifyOutput.getOutputs().size(), is(1));
        assertThat(verifyOutput.getOutputs().getFirst().getRow().get("TRANSACTION_COUNT"), is(1));
    }

    @Override
    protected String getUrl() {
        return "jdbc:db2://localhost:5023/testdb";
    }

    @Override
    protected String getUsername() {
        return "db2inst1";
    }

    @Override
    protected String getPassword() {
        return "password";
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/db2_queries.sql");
    }

    private static volatile boolean initialized = false;

    @Override
    @BeforeEach
    public void init() throws IOException, URISyntaxException, SQLException {
        if (!initialized) {
            synchronized (DB2QueriesTest.class) {
                if (!initialized) {
                    initDatabase();
                    initialized = true;
                }
            }
        }
    }
}
