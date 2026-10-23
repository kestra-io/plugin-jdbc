package io.kestra.plugin.jdbc.sqlserver;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class SqlServerExecutionDetailsTest {
    private static final String URL = "jdbc:sqlserver://localhost:41433;trustServerCertificate=true";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void bracketedTablesAndRowsAffected() throws Exception {
        Queries.builder()
            .url(Property.ofValue(URL)).username(Property.ofValue("sa")).password(Property.ofValue("Sqls3rv3r_Pa55word!"))
            .sql(Property.ofValue("DROP TABLE IF EXISTS exec_t; CREATE TABLE exec_t (id int); INSERT INTO exec_t VALUES (1), (2);"))
            .build()
            .run(runContextFactory.of(Map.of()));

        var output = Query.builder()
            .url(Property.ofValue(URL)).username(Property.ofValue("sa")).password(Property.ofValue("Sqls3rv3r_Pa55word!"))
            .sql(Property.ofValue("UPDATE [dbo].[exec_t] SET id = id + 1"))
            .build()
            .run(runContextFactory.of(Map.of()));

        assertThat(output.getRowsAffected(), is(2L));
        assertThat(output.getTables(), hasSize(1));
        assertThat(output.getTables().getFirst().getName(), is("exec_t"));
        assertThat(output.getTables().getFirst().getSchema(), is("dbo"));
    }
}
