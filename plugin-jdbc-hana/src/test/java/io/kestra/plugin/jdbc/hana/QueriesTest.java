package io.kestra.plugin.jdbc.hana;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static io.kestra.core.models.tasks.common.FetchType.FETCH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class QueriesTest extends HanaTestUtils {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void multiStatementQuery() throws Exception {
        RunContext runContext = runContextFactory.of();

        Queries task = Queries.builder()
            .url(Property.ofValue(getJdbcUrl()))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .fetchType(Property.ofValue(FETCH))
            .sql(Property.ofValue("""
                SELECT 1 FROM DUMMY;
                SELECT 2 FROM DUMMY;
            """))
            .build();

        AbstractJdbcQueries.MultiQueryOutput output = task.run(runContext);

        assertThat(output.getOutputs(), notNullValue());
        assertThat(output.getOutputs().size(), is(2));
    }
}
