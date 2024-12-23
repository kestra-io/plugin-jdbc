package io.kestra.plugin.jdbc.druid;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.kestra.core.models.tasks.common.FetchType.FETCH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class DruidQueriesTest {
    @Inject
    RunContextFactory runContextFactory;

    @BeforeAll
    public static void startServer() throws Exception {
        DruidTestHelper.initServer();
    }

    @Test
    void insertAndQuery() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Queries task = Queries.builder()
                .url(Property.of("jdbc:avatica:remote:url=http://localhost:8888/druid/v2/sql/avatica/;transparent_reconnection=true"))
                .fetchType(Property.of(FETCH))
                .timeZoneId(Property.of("Europe/Paris"))
                .parameters(Property.of(Map.of(
                    "limitLow", 2,
                    "limitHigh", 10))
                )
                .sql(Property.of("""
                        select * from products limit :limitLow;
                        select * from products limit :limitHigh;
                        """))
                .build();

        AbstractJdbcQueries.MultiQueryOutput runOutput = task.run(runContext);
        assertThat(runOutput.getOutputs(), notNullValue());
        assertThat(runOutput.getOutputs().getFirst().getRows().size(), is(2));
        assertThat(runOutput.getOutputs().getLast().getRows().size(), is(10));
    }
}

