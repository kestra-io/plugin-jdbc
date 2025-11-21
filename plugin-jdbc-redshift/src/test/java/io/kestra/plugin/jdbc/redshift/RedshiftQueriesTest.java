package io.kestra.plugin.jdbc.redshift;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import io.micronaut.context.annotation.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.kestra.core.models.tasks.common.FetchType.FETCH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@Disabled("no server for unit test")
public class RedshiftQueriesTest extends AbstractRdbmsTest {
    @Value("${redshift.url}")
    protected String url;

    @Value("${redshift.user}")
    protected String user;

    @Value("${redshift.password}")
    protected String password;

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
        assertThat("employee selected", employees.getFirst().get("age"), is(BigDecimal.valueOf(45)));
        assertThat("employee selected", employees.getFirst().get("firstname"), is("John"));
        assertThat("employee selected", employees.getFirst().get("lastname"), is("Doe"));

        List<Map<String, Object>>laptops = runOutput.getOutputs().getLast().getRows();
        assertThat("laptops", laptops, notNullValue());
        assertThat("laptops", laptops.size(), is(1));
        assertThat("selected laptop", laptops.getFirst().get("brand"), is("Apple"));
    }

    @Override
    protected String getUrl() {
        return this.url;
    }

    @Override
    protected String getUsername() {
        return this.user;
    }

    @Override
    protected String getPassword() {
        return this.password;
    }

    @Override
    @BeforeEach
    public void init() throws IOException, URISyntaxException, SQLException {
        initDatabase();
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
         executeSqlScript("scripts/redshift_queries.sql");
    }
}
