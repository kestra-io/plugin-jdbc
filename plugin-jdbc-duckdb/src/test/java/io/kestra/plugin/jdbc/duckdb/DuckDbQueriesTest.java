package io.kestra.plugin.jdbc.duckdb;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import jakarta.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.FileInputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static io.kestra.core.models.tasks.common.FetchType.FETCH;
import static io.kestra.core.models.tasks.common.FetchType.FETCH_ONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * See :
 * - https://duckdb.org/docs/sql/data_types/overview
 */
@KestraTest
public class DuckDbQueriesTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void multiSelect() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Queries task = Queries.builder()
            .fetchType(FETCH)
            .timeZoneId("Europe/Paris")
            .parameters(Property.of(Map.of("age", "30")))
            .sql("""
                CREATE TABLE employee (id INTEGER PRIMARY KEY, name VARCHAR, age INTEGER);
                CREATE TABLE laptop (id INTEGER PRIMARY KEY, brand VARCHAR, model VARCHAR);
                
                INSERT INTO employee(id, name, age) VALUES (1, 'John', 25), (2, 'Bryan', 35);
                INSERT INTO laptop(id, brand, model) VALUES (1, 'Apple', 'MacBook M3 16'), (2, 'LG', 'Gram');
                
                SELECT * FROM employee where age > :age;
                SELECT * FROM laptop;
                """)
            .build();

        Queries.Output runOutput = task.run(runContext);
        assertThat(runOutput.getOutputs().getFirst(), notNullValue());
        assertThat(runOutput.getOutputs().getLast(), notNullValue());

        assertThat("employee selected", runOutput.getOutputs().getFirst().getRows().size(), is(1));
        assertThat("employee name with age > 30", runOutput.getOutputs().getFirst().getRows().getFirst().get("name"), is("Bryan"));
        assertThat("laptops size", runOutput.getOutputs().getLast().getRows().size(), is(2));
    }

    @Test
    void multiSelectFromExistingFileInUrl() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        URL resource = DuckDbQueriesTest.class.getClassLoader().getResource("db/duck.db");

        Queries task = Queries.builder()
            .fetchType(FETCH)
            .timeZoneId("Europe/Paris")
            .parameters(Property.of(Map.of("age", "30")))
            .url("jdbc:duckdb:"+ Objects.requireNonNull(resource).getPath())
            .sql("""
                CREATE TABLE employee (id INTEGER PRIMARY KEY, name VARCHAR, age INTEGER);
                CREATE TABLE laptop (id INTEGER PRIMARY KEY, brand VARCHAR, model VARCHAR);
                
                INSERT INTO employee(id, name, age) VALUES (1, 'John', 25), (2, 'Bryan', 35);
                INSERT INTO laptop(id, brand, model) VALUES (1, 'Apple', 'MacBook M3 16'), (2, 'LG', 'Gram');
                
                SELECT * FROM employee where age > :age;
                SELECT * FROM laptop;
                """)
            .build();

        Queries.Output runOutput = task.run(runContext);
        assertThat(runOutput.getOutputs().getFirst(), notNullValue());
        assertThat(runOutput.getOutputs().getLast(), notNullValue());

        assertThat("employee selected", runOutput.getOutputs().getFirst().getRows().size(), is(1));
        assertThat("employee name with age > 30", runOutput.getOutputs().getFirst().getRows().getFirst().get("name"), is("Bryan"));
        assertThat("laptops size", runOutput.getOutputs().getLast().getRows().size(), is(2));
    }


    static Stream<String> nullOrFilledDuckDbUrl() {
        return Stream.of(
            null,
            "jdbc:duckdb:/tmp/" + IdUtils.create() + ".db"
        );
    }

    @ParameterizedTest
    @MethodSource("nullOrFilledDuckDbUrl") // six numbers
    void inputOutputFiles(String url) throws Exception {
        URI source = storageInterface.put(
            null,
            new URI("/" + IdUtils.create()),
            new FileInputStream(new File(Objects.requireNonNull(DuckDbQueriesTest.class.getClassLoader()
                    .getResource("full.csv"))
                .toURI())
            )
        );
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Queries.QueriesBuilder<?, ?> builder = Queries.builder()
            .timeZoneId("Europe/Paris")
            .inputFiles(Map.of("in.csv", source.toString()))
            .outputFiles(List.of("out"))
            .fetchType(FETCH_ONE)
            .sql("""
                CREATE TABLE new_tbl AS SELECT * FROM read_csv_auto('{{workingDir}}/in.csv', header=True);
                COPY (SELECT id, name FROM new_tbl) TO '{{ outputFiles.out }}' (HEADER, DELIMITER ',');
                SELECT COUNT(id) as count FROM new_tbl;
                SELECT name FROM new_tbl ORDER BY name LIMIT 1;
                """
            );

        if (url != null) {
            builder.url(url);
        }

        Queries.Output runOutput = builder
            .build()
            .run(runContext);

        assertThat(runOutput.getOutputs().size(), is(2));
        assertThat(runOutput.getOutputs().getFirst(), notNullValue());
        assertThat("Query count", runOutput.getOutputs().getFirst().getRow().get("count"), is(10L));
        assertThat("Query name", runOutput.getOutputs().getLast().getRow().get("name"), is("Ailane"));

        assertThat(
            IOUtils.toString(storageInterface.get(null, runOutput.getOutputFiles().get("out")), Charsets.UTF_8),
            is( "id,name\n" +
                "4814976,Viva\n" +
                "1010871,Voomm\n" +
                "6782048,Ailane\n" +
                "989670,Livetube\n" +
                "1696964,Quinu\n" +
                "8761731,Quamba\n" +
                "5768523,Tagcat\n" +
                "6702408,Roomm\n" +
                "3896245,Flashspan\n" +
                "946749,Browsebug\n"
            )
        );
    }
}
