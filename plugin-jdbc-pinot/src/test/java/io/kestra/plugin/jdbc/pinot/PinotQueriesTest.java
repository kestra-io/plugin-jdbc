package io.kestra.plugin.jdbc.pinot;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static io.kestra.core.models.tasks.common.FetchType.FETCH_ONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * See :
 * - https://docs.pinot.apache.org/configuration-reference/schema
 */
@KestraTest
class PinotQueriesTest {
    @Inject
    RunContextFactory runContextFactory;

    @Test
    void oneSelect() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Queries task = Queries.builder()
            .url(Property.ofValue("jdbc:pinot://localhost:49000"))
            .fetchType(Property.ofValue(FETCH_ONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("""
                select count(*) as count from airlineStats;
                """))
            .build();

        AbstractJdbcQueries.MultiQueryOutput runOutput = task.run(runContext);
        assertThat(runOutput.getOutputs().getFirst().getRow(), notNullValue());
        assertThat(runOutput.getOutputs().getFirst().getRow().get("count"), is(9746L));
    }

    @Test
    void multiSelect() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Queries task = Queries.builder()
            .url(Property.ofValue("jdbc:pinot://localhost:49000"))
            .fetchType(Property.ofValue(FETCH_ONE))
            .timeZoneId(Property.ofValue("Europe/Paris"))
            .sql(Property.ofValue("""
            select count(*) as count from airlineStats;
            select min(ActualElapsedTime) as min_actual_elapsed_time from airlineStats;
            select max(ActualElapsedTime) as max_actual_elapsed_time from airlineStats;
            select count(*) as airline_19805_count from airlineStats where AirlineID = 19805;
            """))
            .build();

        AbstractJdbcQueries.MultiQueryOutput runOutput = task.run(runContext);

        // 4 SELECT => 4 outputs
        assertThat(runOutput.getOutputs().size(), is(4));

        // Output 1
        assertThat(runOutput.getOutputs().get(0).getRow(), notNullValue());
        assertThat(runOutput.getOutputs().get(0).getRow().get("count"), is(9746L));

        // Output 2 (min)
        assertThat(runOutput.getOutputs().get(1).getRow(), notNullValue());
        assertThat(runOutput.getOutputs().get(1).getRow().get("min_actual_elapsed_time"), notNullValue());

        // Output 3 (max)
        assertThat(runOutput.getOutputs().get(2).getRow(), notNullValue());
        assertThat(runOutput.getOutputs().get(2).getRow().get("max_actual_elapsed_time"), notNullValue());

        // Output 4 (filtered count)
        assertThat(runOutput.getOutputs().get(3).getRow(), notNullValue());
        assertThat(runOutput.getOutputs().get(3).getRow().get("airline_19805_count"), notNullValue());
    }
}
