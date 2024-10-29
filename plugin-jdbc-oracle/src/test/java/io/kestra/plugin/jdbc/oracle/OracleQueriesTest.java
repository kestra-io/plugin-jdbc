package io.kestra.plugin.jdbc.oracle;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
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

/**
 * See :
 * - https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT1843
 */
@KestraTest
public class OracleQueriesTest extends AbstractRdbmsTest {
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
        assertThat("employee selected", employees.getFirst().get("AGE"), is(BigDecimal.valueOf(45)));
        assertThat("employee selected", employees.getFirst().get("FIRSTNAME"), is("John"));
        assertThat("employee selected", employees.getFirst().get("LASTNAME"), is("Doe"));

        List<Map<String, Object>>laptops = runOutput.getOutputs().getLast().getRows();
        assertThat("laptops", laptops, notNullValue());
        assertThat("laptops", laptops.size(), is(1));
        assertThat("selected laptop", laptops.getFirst().get("BRAND"), is("Apple"));
    }

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
                CREATE TABLE test_rollback (n NUMBER);
                INSERT INTO test_rollback (n) VALUES (1);
                SELECT COUNT(n) as TEST_ROLLBACK_COUNT FROM test_rollback;
                """)
            .build();

        AbstractJdbcQueries.MultiQueryOutput runOutput = queriesPass.run(runContext);
        assertThat(runOutput.getOutputs().size(), is(1));
        assertThat(runOutput.getOutputs().getFirst().getRow().get("TEST_ROLLBACK_COUNT"), is(BigDecimal.valueOf(1)));

        //Queries should fail due to bad sql
        Queries insertsFail = Queries.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchType(FETCH_ONE)
            .timeZoneId("Europe/Paris")
            .sql("""
                INSERT INTO test_rollback (n) VALUES (2);
                INSERT INTO test_rollback (n) VALUES ('hello');
                """) //Try inserting before failing
            .build();

        assertThrows(Exception.class, () -> insertsFail.run(runContext));

        //Final query to verify the amount of updated rows
        Queries verifyQuery = Queries.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchType(FETCH_ONE)
            .timeZoneId("Europe/Paris")
            .sql("""
                SELECT COUNT(n) as TEST_ROLLBACK_COUNT FROM test_rollback;
                """) //Try inserting before failing
            .build();

        AbstractJdbcQueries.MultiQueryOutput verifyOutput = verifyQuery.run(runContext);
        assertThat(verifyOutput.getOutputs().size(), is(1));
        assertThat(verifyOutput.getOutputs().getFirst().getRow().get("TEST_ROLLBACK_COUNT"), is(BigDecimal.valueOf(1)));
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
                CREATE TABLE test_transaction (n NUMBER);
                INSERT INTO test_transaction (n) VALUES (1);
                INSERT INTO test_transaction (n) VALUES ('random');
                INSERT INTO test_transaction (n) VALUES (2);
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
                SELECT COUNT(n) as TRANSACTION_COUNT FROM test_transaction;
                """) //Try inserting before failing
            .build();

        AbstractJdbcQueries.MultiQueryOutput verifyOutput = verifyQuery.run(runContext);
        assertThat(verifyOutput.getOutputs().size(), is(1));
        assertThat(verifyOutput.getOutputs().getFirst().getRow().get("TRANSACTION_COUNT"), is(BigDecimal.valueOf(1)));
    }

    @Override
    protected String getUrl() {
        return "jdbc:oracle:thin:@localhost:49161:XE";
    }

    @Override
    protected String getUsername() {
        return "system";
    }

    @Override
    protected String getPassword() {
        return "oracle";
    }


    @Override
    @BeforeEach
    public void init() throws IOException, URISyntaxException, SQLException {
        initDatabase();
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        deleteDb( "employee");
        deleteDb("laptop");
        deleteDb("test_rollback");
        deleteDb("test_transaction");

        executeSqlScript("scripts/oracle_queries.sql");
    }

    private void deleteDb(String dbName) {
        try {
            RunScript.execute(getConnection(), new StringReader("DROP TABLE " + dbName + ";"));
        } catch (Exception ignored) {
            //Ignore if DB does not exists
        }
    }
}
