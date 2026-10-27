package io.kestra.plugin.jdbc.sqlite;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.tenant.TenantService;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import io.kestra.plugin.jdbc.SqlTableExtractor;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class SqliteExecutionDetailsTest extends AbstractRdbmsTest {

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readIon(URI uri) throws Exception {
        var rows = new ArrayList<Map<String, Object>>();
        try (var input = storageInterface.get(TenantService.MAIN_TENANT, null, uri)) {
            FileSerde.read(input, row -> rows.add((Map<String, Object>) row));
        }
        return rows;
    }

    @Test
    void queryExposesRowsAffectedDurationAndTables() throws Exception {
        var runContext = runContextFactory.of(ImmutableMap.of());

        var output = Query.builder()
            .url(Property.ofValue(getUrl()))
            .sql(Property.ofValue("INSERT INTO lite_types (id, text_column) SELECT id + 1000, text_column FROM lite_types WHERE id = 1"))
            .build()
            .run(runContext);

        assertThat(output.getRowsAffected(), is(1L));
        assertThat(output.getDurationMs(), greaterThanOrEqualTo(0L));
        assertThat(output.getWarnings(), is(0));
        assertThat(output.getBytes(), nullValue());
        assertThat(output.getStatement(), nullValue());
        assertThat(output.getTablesTruncated(), nullValue());
        assertThat(
            output.getTables().stream().map(t -> t.getAccess() + ":" + t.getName()).toList(),
            containsInAnyOrder("WRITE:lite_types", "READ:lite_types")
        );
        assertThat(output.getTables().getFirst().getConfidence(), is(SqlTableExtractor.Confidence.HIGH));

        Query.builder()
            .url(Property.ofValue(getUrl()))
            .sql(Property.ofValue("DELETE FROM lite_types WHERE id = 1001"))
            .build()
            .run(runContext);
    }

    @Test
    void storeExposesBytesAndColumnStats() throws Exception {
        var runContext = runContextFactory.of(ImmutableMap.of());

        var output = Query.builder()
            .url(Property.ofValue(getUrl()))
            .fetchType(Property.ofValue(FetchType.STORE))
            .columnStats(Property.ofValue(true))
            .columnStatsSampleRows(Property.ofValue(10))
            .outputStatement(Property.ofValue(true))
            .sql(Property.ofValue("SELECT id, text_column FROM lite_types"))
            .build()
            .run(runContext);

        assertThat(output.getRowsAffected(), nullValue());
        assertThat(output.getBytes(), greaterThan(0L));
        assertThat(output.getStatement(), is("SELECT id, text_column FROM lite_types"));
        assertThat(output.getColumnStats(), notNullValue());

        var stats = readIon(output.getColumnStats());
        assertThat(stats.stream().map(s -> s.get("column")).toList(), contains("id", "text_column"));
        var id = stats.getFirst();
        assertThat(((Number) id.get("nulls")).longValue(), is(0L));
        assertThat(((Number) id.get("min")).longValue(), is(1L));
        assertThat(stats.get(1).get("minLen"), notNullValue());
    }

    @Test
    void fetchOneSkipsColumnStats() throws Exception {
        var runContext = runContextFactory.of(ImmutableMap.of());

        var output = Query.builder()
            .url(Property.ofValue(getUrl()))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .columnStats(Property.ofValue(true))
            .sql(Property.ofValue("SELECT id FROM lite_types"))
            .build()
            .run(runContext);

        assertThat(output.getRow(), notNullValue());
        assertThat(output.getColumnStats(), nullValue());
    }

    @Test
    void invalidSampleRowsIsRejected() {
        var runContext = runContextFactory.of(ImmutableMap.of());

        var task = Query.builder()
            .url(Property.ofValue(getUrl()))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .columnStats(Property.ofValue(true))
            .columnStatsSampleRows(Property.ofValue(0))
            .sql(Property.ofValue("SELECT id FROM lite_types"))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("columnStatsSampleRows"));
    }

    @Test
    void statementMasksSecretsAndOutputsNeverContainCredentials() throws Exception {
        var runContext = runContextFactory.of(Map.of("flow", Map.of(
            "id", "secret_flow",
            "namespace", "io.kestra.tests",
            "tenantId", TenantService.MAIN_TENANT
        )));

        var output = Query.builder()
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue("someuser"))
            .password(Property.ofValue("hunter2-password"))
            .outputStatement(Property.ofValue(true))
            .sql(Property.ofExpression("SELECT '{{ secret('JDBC_MASK_TEST') }}' AS token"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build()
            .run(runContext);

        assertThat(output.getRow().get("token"), is("s3cr3t-value"));
        assertThat(output.getStatement(), is("SELECT '***' AS token"));

        var json = JacksonMapper.ofJson().writeValueAsString(output);
        assertThat(json, not(containsString("hunter2-password")));
        assertThat(json, not(containsString("someuser")));
    }

    @Test
    void statementMasksNamespacedSecretsAndTheirBase64Form() throws Exception {
        var runContext = runContextFactory.of(Map.of("flow", Map.of(
            "id", "secret_flow",
            "namespace", "io.kestra.tests",
            "tenantId", TenantService.MAIN_TENANT
        )));

        var output = Query.builder()
            .url(Property.ofValue(getUrl()))
            .outputStatement(Property.ofValue(true))
            .sql(Property.ofExpression("SELECT '{{ secret('JDBC_MASK_TEST', namespace='io.kestra.tests') }}' AS token, 'czNjcjN0LXZhbHVl' AS encoded"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build()
            .run(runContext);

        assertThat(output.getStatement(), is("SELECT '***' AS token, '***' AS encoded"));
    }

    @Test
    void queriesExposeStatementsFileAndTotals() throws Exception {
        var runContext = runContextFactory.of(ImmutableMap.of());

        var output = Queries.builder()
            .url(Property.ofValue(getUrl()))
            .fetchType(Property.ofValue(FetchType.STORE))
            .sql(Property.ofValue("""
                CREATE TABLE IF NOT EXISTS exec_details (id INTEGER, name TEXT);
                DELETE FROM exec_details;
                INSERT INTO exec_details VALUES (1, 'a'), (2, 'b');
                SELECT * FROM exec_details;
                DROP TABLE exec_details;
                """))
            .build()
            .run(runContext);

        assertThat(output.getStatementCount(), is(5L));
        assertThat(output.getRowsAffected(), greaterThanOrEqualTo(2L));
        assertThat(output.getBytes(), greaterThan(0L));
        assertThat(output.getDurationMs(), greaterThanOrEqualTo(0L));

        var statements = readIon(output.getStatements());
        assertThat(statements.stream().map(s -> s.get("kind")).toList(), contains("CREATE", "DELETE", "INSERT", "SELECT", "DROP"));
        assertThat(((Number) statements.get(2).get("rowsAffected")).longValue(), is(2L));
        assertThat(statements.get(3).get("rowsAffected"), nullValue());
        assertThat(
            output.getTables().stream().map(t -> t.getAccess() + ":" + t.getName()).toList(),
            containsInAnyOrder("CREATE:exec_details", "WRITE:exec_details", "READ:exec_details", "DROP:exec_details")
        );
    }

    @Override
    protected String getUrl() {
        return TestUtils.url();
    }

    @Override
    protected String getUsername() {
        return TestUtils.username();
    }

    @Override
    protected String getPassword() {
        return TestUtils.password();
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/sqlite.sql");
    }
}
