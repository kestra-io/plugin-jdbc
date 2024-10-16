package io.kestra.plugin.jdbc.mysql;

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
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.kestra.core.models.tasks.common.FetchType.FETCH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * See : https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-type-conversions.html
 */
@KestraTest
public class QueriesMysqlTest extends AbstractRdbmsTest {

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
                SELECT firstName, lastName FROM employee;
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
                SELECT firstName, lastName, age FROM employee where age > :age and age < :age + 10;
                SELECT brand, model FROM laptop where brand = :brand and cpu_frequency > :cpu_frequency;
                """)
            .parameters(Property.of(parameters))
            .build();

        AbstractJdbcQueries.MultiQueryOutput runOutput = taskGet.run(runContext);
        assertThat(runOutput.getOutputs().size(), is(2));

        List<Map<String, Object>> employees = runOutput.getOutputs().get(0).getRows();
        assertThat("employees", employees, notNullValue());
        assertThat("employees", employees.size(), is(1));
        assertThat("employee selected", employees.getFirst().get("age"), is(45));
        assertThat("employee selected", employees.getFirst().get("firstName"), is("John"));
        assertThat("employee selected", employees.getFirst().get("lastName"), is("Doe"));

        List<Map<String, Object>>laptops = runOutput.getOutputs().get(1).getRows();
        assertThat("laptops", laptops, notNullValue());
        assertThat("laptops", laptops.size(), is(1));
        assertThat("selected laptop", laptops.getFirst().get("brand"), is("Apple"));
    }

    @Override
    protected String getUrl() {
        return "jdbc:mysql://127.0.0.1:64790/kestra";
    }

    @Override
    protected String getUsername() {
        return "root";
    }

    @Override
    protected String getPassword() {
        return "mysql_passwd";
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/mysql_queries.sql");
    }

    @Override
    @BeforeEach
    public void init() throws IOException, URISyntaxException, SQLException {
        initDatabase();
    }
}
