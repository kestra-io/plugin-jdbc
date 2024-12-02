package io.kestra.plugin.jdbc.postgresql;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.kestra.core.models.tasks.common.FetchType.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class QueriesPostgresTest extends AbstractRdbmsTest {

    @Test
    void testMultiSelect() throws Exception {
        RunContext runContext = runContextFactory.of(Collections.emptyMap());

        Queries taskGet = Queries.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchType(FETCH)
            .timeZoneId("Europe/Paris")
            .sql("""
                SELECT first_name, last_name FROM employee;
                SELECT brand FROM laptop;
                """)
            .build();

        AbstractJdbcQueries.MultiQueryOutput runOutput = taskGet.run(runContext);
        assertThat(runOutput.getOutputs().size(), is(2));
        assertThat(runOutput.getOutputs().get(0), notNullValue());
        assertThat(runOutput.getOutputs().get(1), notNullValue());
    }

    @Test
    void testMultiSelectWithParameters() throws Exception {
        RunContext runContext = runContextFactory.of(Collections.emptyMap());

        Map<String, Object> parameters = Map.of(
            "age", 40,
            "brand", "Apple",
            "cpu_frequency", 1.5
        );

        Queries taskGet = Queries.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchType(FETCH)
            .timeZoneId("Europe/Paris")
            .sql("""
                SELECT first_name, last_name, age FROM employee where age > :age and age < :age + 10;
                SELECT brand, model FROM laptop where brand = :brand and cpu_frequency > :cpu_frequency;
                """)
            .parameters(Property.of(parameters))
            .build();

        AbstractJdbcQueries.MultiQueryOutput runOutput = taskGet.run(runContext);
        assertThat(runOutput.getOutputs().size(), is(2));

        List<Map<String, Object>> employees = runOutput.getOutputs().getFirst().getRows();
        assertThat("employees", employees, notNullValue());
        assertThat("employees", employees.size(), is(1));
        assertThat("employee selected", employees.getFirst().get("age"), is(45));
        assertThat("employee selected", employees.getFirst().get("first_name"), is("John"));
        assertThat("employee selected", employees.getFirst().get("last_name"), is("Doe"));

        List<Map<String, Object>>laptops = runOutput.getOutputs().getLast().getRows();
        assertThat("laptops", laptops, notNullValue());
        assertThat("laptops", laptops.size(), is(1));
        assertThat("selected laptop", laptops.getFirst().get("brand"), is("Apple"));
    }

    @Test
    void testMultiSelectWithParametersWithColonCloseTo() throws Exception {
        RunContext runContext = runContextFactory.of(Collections.emptyMap());

        Queries setup = Queries.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchType(NONE)
            .timeZoneId("Europe/Paris")
            .sql("""
                DROP TABLE IF EXISTS myusers CASCADE;
                      CREATE TABLE myusers (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        email VARCHAR(255) UNIQUE NOT NULL,
                        last_login TIMESTAMP
                      );
                  DROP TABLE IF EXISTS mylogs;
                  CREATE TABLE mylogs (
                    log_id SERIAL PRIMARY KEY,
                    user_email VARCHAR(255) NOT NULL,
                    action VARCHAR(255) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    FOREIGN KEY (user_email) REFERENCES myusers(email)
                  );
                """)
            .build();
        setup.run(runContext);

        Map<String, Object> parameters = Map.of(
            "name1", "John Doe",
            "name2", "Jane Smith",
            "name3", "Alice Johnson",
            "email1", "johndoe@example.com",
            "email2", "janesmith@example.com",
            "email3", "alicejohnson@example.com",
            "action1", "login",
            "action2", "login",
            "action3", "login"
        );

        Queries insertAndSelect = Queries.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchType(FETCH)
            .timeZoneId("Europe/Paris")
            .sql("""
                INSERT INTO myusers (name, email, last_login)
                      VALUES
                      (:name1, :email1, NOW()),
                      (:name2, :email2, NOW()),
                      (:name3, :email3, NOW());
                INSERT INTO mylogs (user_email, action, timestamp)
                VALUES
                  (:email1, :action1, NOW() - INTERVAL '10 MINUTES'),
                  (:email2, :action2, NOW() - INTERVAL '20 MINUTES'),
                  (:email3, :action3, NOW() - INTERVAL '30 MINUTES');
                SELECT * FROM mylogs;
                SELECT * FROM myusers;
                """)
            .parameters(Property.of(parameters))
            .build();

        Queries.MultiQueryOutput output = insertAndSelect.run(runContext);
        assertThat(output.getOutputs().size(), is(2));
    }

    @Test
    void testMultiQueriesOnlySelectOutputs() throws Exception {
        RunContext runContext = runContextFactory.of(Collections.emptyMap());

        Queries taskGet = Queries.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchType(FETCH_ONE)
            .timeZoneId("Europe/Paris")
            .sql("""
                DROP TABLE IF EXISTS animals;
                CREATE TABLE animals (
                     id SERIAL,
                     name VARCHAR(30) NOT NULL,
                     PRIMARY KEY (id)
                );
                INSERT INTO animals (name) VALUES ('cat'),('dog');
                SELECT COUNT(id) as animals_count FROM animals;
                INSERT INTO animals (name) VALUES ('ostrich'),('snake'),('whale');
                SELECT COUNT(id) as animals_count FROM animals;
                """)
            .build();

        AbstractJdbcQueries.MultiQueryOutput runOutput = taskGet.run(runContext);
        assertThat(runOutput.getOutputs().size(), is(2));
        assertThat(runOutput.getOutputs().getFirst().getRow().get("animals_count"), is(2L));
        assertThat(runOutput.getOutputs().getLast().getRow().get("animals_count"), is(5L));
    }

    @Test
    void testMultiQueriesTransactionalShouldRollback() throws Exception {
        long expectedUpdateNumber = 1L;
        RunContext runContext = runContextFactory.of(Collections.emptyMap());

        //Queries should pass in a transaction
        Queries queriesPass = Queries.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchType(FETCH_ONE)
            .timeZoneId("Europe/Paris")
            .sql("""
                INSERT INTO test_transaction (name) VALUES ('test_1');
                SELECT COUNT(id) as transaction_count FROM test_transaction;
                """)
            .build();

        AbstractJdbcQueries.MultiQueryOutput runOutput = queriesPass.run(runContext);
        assertThat(runOutput.getOutputs().size(), is(1));
        assertThat(runOutput.getOutputs().getFirst().getRow().get("transaction_count"), is(expectedUpdateNumber));

        //Queries should fail due to bad sql
        Queries queriesFail = Queries.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchType(FETCH_ONE)
            .timeZoneId("Europe/Paris")
            .sql("""
                INSERT INTO test_transaction (name) VALUES ('test_2');
                INSERT INTO test_transaction (name) VALUES (1000f);
                """) //Try inserting before failing
            .build();

        assertThrows(Exception.class, () -> queriesFail.run(runContext));

        //Final query to verify the amount of updated rows
        Queries verifyQuery = Queries.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchType(FETCH_ONE)
            .timeZoneId("Europe/Paris")
            .sql("""
                SELECT COUNT(id) as transaction_count FROM test_transaction;
                """) //Try inserting before failing
            .build();

        AbstractJdbcQueries.MultiQueryOutput verifyOutput = verifyQuery.run(runContext);
        assertThat(verifyOutput.getOutputs().size(), is(1));
        assertThat(verifyOutput.getOutputs().getFirst().getRow().get("transaction_count"), is(expectedUpdateNumber));
    }

    @Test
    void testMultiQueriesNonTransactionalShouldNotRollback() throws Exception {
        RunContext runContext = runContextFactory.of(Collections.emptyMap());

        //Queries should pass in a transaction
        Queries queriesFail = Queries.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchType(FETCH_ONE)
            .timeZoneId("Europe/Paris")
            .transaction(Property.of(false)) //No rollback on failure
            .sql("""
                INSERT INTO test_transaction (name) VALUES ('test_no_rollback_success_1');
                INSERT INTO test_transaction (name) VALUES ('test_no_rollback_success_2');
                INSERT INTO test_transaction (name) VALUES (10f);
                INSERT INTO test_transaction (name) VALUES ('test_no_rollback_fail_1');
                INSERT INTO test_transaction (name) VALUES ('test_no_rollback_fail_2');
                """) //Expect failure with 2 inserts
            .build();

        assertThrows(Exception.class, () -> queriesFail.run(runContext));

        //Final query to verify the amount of updated rows
        Queries verifyQuery = Queries.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchType(FETCH)
            .timeZoneId("Europe/Paris")
            .sql("""
                SELECT name FROM test_transaction;
                """)
            .build();

        AbstractJdbcQueries.MultiQueryOutput verifyOutput = verifyQuery.run(runContext);
        List<String> names = verifyOutput.getOutputs().getFirst().getRows()
            .stream().map(m -> (String) m.get("name"))
            .filter(name -> name.startsWith("test_no_rollback"))
            .toList();

        assertThat(names.size(), is(2));
        assertThat(names, containsInAnyOrder("test_no_rollback_success_1", "test_no_rollback_success_2"));
    }

    @Override
    protected String getUrl() {
        return "jdbc:postgresql://127.0.0.1:56983/";
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
        Properties props = new Properties();
        props.put("jdbc.url", getUrl());
        props.put("user", getUsername());
        props.put("password", getPassword());

        try {
            PostgresService.handleSsl(props, runContextFactory.of(), new PostgresConnection());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return DriverManager.getConnection(props.getProperty("jdbc.url"), props);
    }

    public static class PostgresConnection implements PostgresConnectionInterface {
        @Override
        public String getUrl() {
            return "jdbc:postgresql://127.0.0.1:56983/";
        }

        @Override
        public String getUsername() {
            return TestUtils.username();
        }

        @Override
        public String getPassword() {
            return TestUtils.password();
        }

        @Override
        public Boolean getSsl() {
            return null;
        }

        @Override
        public SslMode getSslMode() {
            return null;
        }

        @Override
        public String getSslRootCert() {
            return null;
        }

        @Override
        public String getSslCert() {
            return null;
        }

        @Override
        public String getSslKey() {
            return null;
        }

        @Override
        public String getSslKeyPassword() {
            return null;
        }

        @Override
        public void registerDriver() throws SQLException {

        }
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/postgres_queries.sql");
    }

    @Override
    @BeforeEach
    public void init() throws IOException, URISyntaxException, SQLException {
        initDatabase();
    }
}
