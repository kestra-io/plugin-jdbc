package io.kestra.plugin.jdbc.access;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static io.kestra.core.models.tasks.common.FetchType.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class AccessQueriesTest extends AbstractRdbmsTest {

    private String currentDbPath;
    private String currentUrl;

    @Test
    void testMultiSelectWithParameters() throws Exception {
        RunContext runContext = runContextFactory.of(Collections.emptyMap());

        Queries task = Queries.builder()
            .url(Property.ofValue(getUrl()))
            .fetchType(Property.ofValue(FETCH))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("""
                SELECT firstName, lastName, age FROM employee WHERE age > :minAge;
                SELECT brand, model FROM laptop WHERE brand = :brand;
                """))
            .parameters(Property.ofValue(Map.of("minAge", 40, "brand", "Apple")))
            .build();

        AbstractJdbcQueries.MultiQueryOutput runOutput = task.run(runContext);
        assertThat(runOutput.getOutputs().size(), is(2));

        var employees = runOutput.getOutputs().getFirst().getRows();
        assertThat(employees, notNullValue());
        assertThat(employees, not(empty()));

        var laptops = runOutput.getOutputs().getLast().getRows();
        assertThat(laptops, notNullValue());
        assertThat(laptops, not(empty()));
        assertThat(laptops.getFirst().get("brand"), is("Apple"));
    }

    @Test
    void testRollback() throws Exception {
        RunContext runContext = runContextFactory.of(Collections.emptyMap());

        Queries queriesPass = Queries.builder()
            .url(Property.ofValue(getUrl()))
            .fetchType(Property.ofValue(FETCH_ONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("""
                CREATE TABLE test_transaction (id INTEGER);
                INSERT INTO test_transaction (id) VALUES (1);
                SELECT COUNT(id) AS transaction_count FROM test_transaction;
                """))
            .build();

        AbstractJdbcQueries.MultiQueryOutput runOutput = queriesPass.run(runContext);
        assertThat(runOutput.getOutputs().size(), is(1));
        assertThat(((Number) runOutput.getOutputs().getFirst().getRow().get("transaction_count")).intValue(), is(1));
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
        currentDbPath = System.getProperty("java.io.tmpdir") + "/test_access_queries_" + UUID.randomUUID() + ".accdb";
        currentUrl = "jdbc:ucanaccess://" + currentDbPath;
        try {
            TestUtils.createDatabase(currentDbPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try (var conn = DriverManager.getConnection(currentUrl);
             var stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE employee (
                    employee_id INTEGER,
                    firstName VARCHAR(255),
                    lastName VARCHAR(255),
                    age INTEGER
                )
                """);
            stmt.execute("INSERT INTO employee (employee_id, firstName, lastName, age) VALUES (1, 'John', 'Doe', 45)");
            stmt.execute("INSERT INTO employee (employee_id, firstName, lastName, age) VALUES (2, 'Bryan', 'Grant', 33)");
            stmt.execute("INSERT INTO employee (employee_id, firstName, lastName, age) VALUES (3, 'Jude', 'Philips', 25)");
            stmt.execute("INSERT INTO employee (employee_id, firstName, lastName, age) VALUES (4, 'Michael', 'Page', 62)");

            stmt.execute("""
                CREATE TABLE laptop (
                    laptop_id INTEGER,
                    brand VARCHAR(255),
                    model VARCHAR(255),
                    cpu_frequency DOUBLE
                )
                """);
            stmt.execute("INSERT INTO laptop (laptop_id, brand, model, cpu_frequency) VALUES (1, 'Apple', 'MacBookPro M1 13', 2.2)");
            stmt.execute("INSERT INTO laptop (laptop_id, brand, model, cpu_frequency) VALUES (2, 'Apple', 'MacBookPro M3 16', 1.5)");
            stmt.execute("INSERT INTO laptop (laptop_id, brand, model, cpu_frequency) VALUES (3, 'LG', 'Gram', 1.95)");
            stmt.execute("INSERT INTO laptop (laptop_id, brand, model, cpu_frequency) VALUES (4, 'Lenovo', 'ThinkPad', 1.05)");
        }
    }
}
