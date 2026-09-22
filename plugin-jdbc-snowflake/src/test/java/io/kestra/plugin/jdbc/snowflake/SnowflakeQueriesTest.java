package io.kestra.plugin.jdbc.snowflake;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.kestra.core.models.tasks.common.FetchType.FETCH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Needs a live Snowflake account, so it only runs when SNOWFLAKE_HOST is set.
 * Obtain the host with: use role accountadmin; select system$allowlist();
 * then take the "host" of the entry whose "type" is "SNOWFLAKE_DEPLOYMENT".
 */
@KestraTest
@EnabledIfEnvironmentVariable(named = "SNOWFLAKE_HOST", matches = ".+")
public class SnowflakeQueriesTest extends AbstractRdbmsTest {
    protected String host = System.getenv("SNOWFLAKE_HOST");
    protected String username = System.getenv("SNOWFLAKE_USERNAME");
    protected String password = System.getenv("SNOWFLAKE_PASSWORD");
    protected String database = System.getenv().getOrDefault("SNOWFLAKE_DATABASE", "KESTRA");
    protected String warehouse = System.getenv().getOrDefault("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH");

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
            .warehouse(Property.ofValue(warehouse))
            .database(Property.ofValue(database))
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
        assertThat("employee selected", employees.getFirst().get("AGE"), is(BigDecimal.valueOf(45)));
        assertThat("employee selected", employees.getFirst().get("FIRSTNAME"), is("John"));
        assertThat("employee selected", employees.getFirst().get("LASTNAME"), is("Doe"));

        List<Map<String, Object>>laptops = runOutput.getOutputs().getLast().getRows();
        assertThat("laptops", laptops, notNullValue());
        assertThat("laptops", laptops.size(), is(1));
        assertThat("selected laptop", laptops.getFirst().get("BRAND"), is("Apple"));
    }

    @Override
    protected String getUrl() {
        return "jdbc:snowflake://" + this.host + "/?loginTimeout=30";
    }

    @Override
    protected String getUsername() {
        return this.username;
    }

    @Override
    protected String getPassword() {
        return this.password;
    }

    @Override
    protected Connection getConnection() throws SQLException {
        Properties properties = new Properties();
        properties.put("user", getUsername());
        properties.put("password", getPassword());
        properties.put("warehouse", warehouse);
        properties.put("db", database);
        properties.put("schema", "public");

        return DriverManager.getConnection(getUrl(), properties);
    }

    @Override
    @BeforeEach
    public void init() throws IOException, URISyntaxException, SQLException {
        initDatabase();
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/snowflake_queries.sql");
    }
}
