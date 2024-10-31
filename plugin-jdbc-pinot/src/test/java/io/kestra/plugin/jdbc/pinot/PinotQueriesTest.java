package io.kestra.plugin.jdbc.pinot;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.kestra.core.models.tasks.common.FetchType.FETCH_ONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * See :
 * - https://docs.pinot.apache.org/configuration-reference/schema
 */
@KestraTest
class PinotQueriesTest {
    @Inject
    RunContextFactory runContextFactory;

    @Test
    void multiSelect() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Queries task = Queries.builder()
            .url("jdbc:pinot://localhost:49000")
            .fetchType(FETCH_ONE)
            .timeZoneId("Europe/Paris")
            .sql("""
                select count(*) as count from airlineStats;
                """)
            .build();

        AbstractJdbcQueries.MultiQueryOutput runOutput = task.run(runContext);
        assertThat(runOutput.getOutputs().getFirst().getRow(), notNullValue());
        assertThat(runOutput.getOutputs().getFirst().getRow().get("count"), is(9746L));
    }
}
