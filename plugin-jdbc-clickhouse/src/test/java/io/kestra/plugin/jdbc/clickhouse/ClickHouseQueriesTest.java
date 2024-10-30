package io.kestra.plugin.jdbc.clickhouse;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.kestra.core.models.tasks.common.FetchType.FETCH;
import static io.kestra.core.models.tasks.common.FetchType.FETCH_ONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class ClickHouseQueriesTest extends AbstractRdbmsTest {

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
                SELECT firstName, lastName, age FROM employee where age > :age and age < :age + 10;
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
        assertThat("employee selected", employees.getFirst().get("firstName"), is("John"));
        assertThat("employee selected", employees.getFirst().get("lastName"), is("Doe"));

        List<Map<String, Object>>laptops = runOutput.getOutputs().getLast().getRows();
        assertThat("laptops", laptops, notNullValue());
        assertThat("laptops", laptops.size(), is(1));
        assertThat("selected laptop", laptops.getFirst().get("brand"), is("Apple"));
    }

    @Disabled("Test for rollback disabled : Savepoint and rollback are not supported by ClickHouse")
    @Test
    void testRollback() throws Exception {
        RunContext runContext = runContextFactory.of(Collections.emptyMap());

        //Queries should pass in a transaction
        Queries queriesPass = Queries.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchType(FETCH_ONE)
            .timeZoneId("Europe/Paris")
            .sql("""
                DROP TABLE IF EXISTS test_transaction;
                
                CREATE TABLE test_transaction(id Int64)
                ENGINE = MergeTree()
                ORDER BY (id)
                SETTINGS index_granularity = 8192;
                
                INSERT INTO test_transaction (id) VALUES (1);
                SELECT COUNT(id) as transaction_count FROM test_transaction;
                """)
            .build();

        AbstractJdbcQueries.MultiQueryOutput runOutput = queriesPass.run(runContext);
        assertThat(runOutput.getOutputs().size(), is(1));
        assertThat(runOutput.getOutputs().getFirst().getRow().get("transaction_count"), is(1L));

        //Queries should fail due to bad sql
        Queries insertsFail = Queries.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchType(FETCH_ONE)
            .timeZoneId("Europe/Paris")
            .sql("""
                INSERT INTO test_transaction (id) VALUES (2);
                INSERT INTO test_transaction (id) VALUES ('random');
                """) //Try inserting before failing
            .build();

        assertThrows(Exception.class, () -> insertsFail.run(runContext));

        //Final query to verify the amount of updated rows
        Queries verifyQuery = Queries.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchType(FETCH)
            .timeZoneId("Europe/Paris")
            .sql("""
                SELECT COUNT(id) as transaction_count FROM test_transaction;
                SELECT * FROM test_transaction;
                """) //Try inserting before failing
            .build();

        AbstractJdbcQueries.MultiQueryOutput verifyOutput = verifyQuery.run(runContext);
        assertThat(verifyOutput.getOutputs().size(), is(1));
        assertThat(verifyOutput.getOutputs().getFirst().getRow().get("transaction_count"), is(1L));
    }

    @Test
    void testNonTransactionalShouldNotRollback() throws Exception {
        RunContext runContext = runContextFactory.of(Collections.emptyMap());

        //Queries should pass in a transaction
        Queries insertOneAndFail = Queries.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchType(FETCH_ONE)
            .transaction(Property.of(false))
            .timeZoneId("Europe/Paris")
            .sql("""
                DROP TABLE IF EXISTS test_transaction;
                
                CREATE TABLE test_transaction(id Int64)
                ENGINE = MergeTree()
                ORDER BY (id)
                SETTINGS index_granularity = 8192;
                
                INSERT INTO test_transaction (id) VALUES (1);
                INSERT INTO test_transaction (id) VALUES ('random');
                INSERT INTO test_transaction (id) VALUES (2);
                """)
            .build();

        assertThrows(Exception.class, () -> insertOneAndFail.run(runContext));

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
        assertThat(verifyOutput.getOutputs().getFirst().getRow().get("transaction_count"), is(1L));
    }

    @Override
    protected String getUrl() {
        return "jdbc:clickhouse://127.0.0.1:28123/default";
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/clickhouse_queries.sql");
    }

    @Override
    @BeforeEach
    public void init() throws IOException, URISyntaxException, SQLException {
        initDatabase();
    }
}
