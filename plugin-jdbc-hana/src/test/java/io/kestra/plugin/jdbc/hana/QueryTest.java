package io.kestra.plugin.jdbc.hana;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import static org.hamcrest.Matchers.is;

import static io.kestra.core.models.tasks.common.FetchType.FETCH_ONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class QueryTest extends HanaTestUtils {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void simpleSelect() throws Exception {
        String url = getJdbcUrl();

        RunContext runContext = runContextFactory.of();

        Query task = Query.builder()
            .url(Property.ofValue(url))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .fetchType(Property.ofValue(FETCH_ONE))
            .sql(Property.ofValue("SELECT 1 FROM DUMMY"))
            .build();

        AbstractJdbcQuery.Output output = task.run(runContext);

        assertThat(output.getRow(), notNullValue());
    }
}
