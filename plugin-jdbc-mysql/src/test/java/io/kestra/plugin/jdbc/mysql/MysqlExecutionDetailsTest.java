package io.kestra.plugin.jdbc.mysql;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class MysqlExecutionDetailsTest {
    private static final String URL = "jdbc:mysql://127.0.0.1:64790/kestra";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void backtickedTablesAndRowsAffected() throws Exception {
        Queries.builder()
            .url(Property.ofValue(URL)).username(Property.ofValue("root")).password(Property.ofValue("mysql_passwd"))
            .sql(Property.ofValue("DROP TABLE IF EXISTS exec_t; CREATE TABLE exec_t (id int); INSERT INTO exec_t VALUES (1), (2);"))
            .build()
            .run(runContextFactory.of(Map.of()));

        var output = Query.builder()
            .url(Property.ofValue(URL)).username(Property.ofValue("root")).password(Property.ofValue("mysql_passwd"))
            .sql(Property.ofValue("UPDATE `kestra`.`exec_t` SET id = id + 1"))
            .build()
            .run(runContextFactory.of(Map.of()));

        assertThat(output.getRowsAffected(), is(2L));
        assertThat(output.getDurationMs(), greaterThanOrEqualTo(0L));
        assertThat(output.getTables(), hasSize(1));
        assertThat(output.getTables().getFirst().getName(), is("exec_t"));
        assertThat(output.getTables().getFirst().getSchema(), is("kestra"));
    }
}
