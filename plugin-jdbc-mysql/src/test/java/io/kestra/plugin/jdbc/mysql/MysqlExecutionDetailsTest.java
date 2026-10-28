package io.kestra.plugin.jdbc.mysql;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class MysqlExecutionDetailsTest {
    private static final String URL = "jdbc:mysql://127.0.0.1:64790/kestra";

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void everyStatementOfAScriptIsCounted() throws Exception {
        var output = Queries.builder()
            .url(Property.ofValue(URL)).username(Property.ofValue("root")).password(Property.ofValue("mysql_passwd"))
            .sql(Property.ofValue("""
                DROP TABLE IF EXISTS exec_count;
                CREATE TABLE exec_count (id int, v varchar(20));
                INSERT INTO exec_count VALUES (1, 'a'), (2, 'b'), (3, 'c');
                """))
            .build()
            .run(runContextFactory.of(Map.of()));

        assertThat(output.getStatementCount(), is(3L));

        var statements = new ArrayList<Map<String, Object>>();
        try (var input = storageInterface.get(TenantService.MAIN_TENANT, null, output.getStatements())) {
            FileSerde.read(input, row -> statements.add((Map<String, Object>) row));
        }
        assertThat(statements.stream().map(s -> s.get("kind")).toList(), contains("DROP", "CREATE", "INSERT"));
        assertThat(((Number) statements.get(2).get("rowsAffected")).longValue(), is(3L));
    }

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
