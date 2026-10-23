package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class DuckDbExecutionDetailsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void queriesExposeDetailsAndTableKinds() throws Exception {
        var output = Queries.builder()
            .fetchType(Property.ofValue(FetchType.FETCH))
            .sql(Property.ofValue("""
                CREATE TABLE people AS SELECT 1 AS id, 'a' AS name;
                INSERT INTO people VALUES (2, 'b');
                SELECT * FROM people;
                """))
            .build()
            .run(runContextFactory.of(Map.of()));

        assertThat(output.getStatementCount(), is(3L));
        assertThat(output.getRowsAffected(), greaterThanOrEqualTo(1L));
        assertThat(output.getDurationMs(), greaterThanOrEqualTo(0L));
        assertThat(output.getStatements(), notNullValue());
        assertThat(
            output.getTables().stream().map(t -> t.getAccess() + ":" + t.getName() + ":" + t.getKind()).toList(),
            containsInAnyOrder("CREATE:people:TABLE", "WRITE:people:TABLE", "READ:people:TABLE")
        );
    }

    @Test
    void queryExposesDetails() throws Exception {
        var output = Query.builder()
            .fetchType(Property.ofValue(FetchType.STORE))
            .columnStats(Property.ofValue(true))
            .sql(Property.ofValue("SELECT range AS n FROM range(10)"))
            .build()
            .run(runContextFactory.of(Map.of()));

        assertThat(output.getSize(), is(10L));
        assertThat(output.getBytes(), greaterThan(0L));
        assertThat(output.getColumnStats(), notNullValue());
        assertThat(output.getTables(), is(empty()));
    }
}
