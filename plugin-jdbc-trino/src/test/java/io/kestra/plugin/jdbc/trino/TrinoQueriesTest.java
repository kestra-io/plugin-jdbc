package io.kestra.plugin.jdbc.trino;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.AbstractMetricEntry;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;

import static io.kestra.core.models.tasks.common.FetchType.FETCH;
import static io.kestra.core.models.tasks.common.FetchType.FETCH_ONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Multi statement execution, engine query ids, typed connection properties and query stats metrics.
 * The tests expect a Trino coordinator on `localhost:48080`, see the `docker-compose-ci.yml` `trino` service.
 */
@KestraTest
public class TrinoQueriesTest {

    private static final String JDBC_URL = "jdbc:trino://localhost:48080/memory/default";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void queryIdIsExposedOnQueryOutputs() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        Query task = Query.builder()
            .url(Property.ofValue(JDBC_URL))
            .username(Property.ofValue("fake"))
            .sql(Property.ofValue("SELECT 42 AS answer"))
            .fetchType(Property.ofValue(FETCH_ONE))
            .build();

        AbstractJdbcQuery.Output output = task.run(runContext);

        assertThat(output.getQueryId(), notNullValue());
        assertThat(output.getQueryId(), containsString("_"));
    }

    @Test
    void queryIdIsExposedOnEveryQueriesOutput() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        Queries task = Queries.builder()
            .url(Property.ofValue(JDBC_URL))
            .username(Property.ofValue("fake"))
            .sql(Property.ofValue("SELECT 1 AS a; SELECT 2 AS b"))
            .fetchType(Property.ofValue(FETCH))
            .build();

        AbstractJdbcQueries.MultiQueryOutput output = task.run(runContext);

        assertThat(output.getOutputs(), hasSize(2));
        assertThat(output.getOutputs().get(0).getQueryId(), notNullValue());
        assertThat(output.getOutputs().get(1).getQueryId(), notNullValue());
        assertThat(output.getOutputs().get(1).getRows().getFirst().get("b"), is(2));
    }

    @Test
    void queryStatsMetricsAreEmitted() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        Query task = Query.builder()
            .url(Property.ofValue(JDBC_URL))
            .username(Property.ofValue("fake"))
            .sql(Property.ofValue("SELECT * FROM tpch.tiny.orders"))
            .fetchType(Property.ofValue(FETCH))
            .build();

        AbstractJdbcQuery.Output output = task.run(runContext);

        assertThat(output.getQueryId(), notNullValue());

        var metrics = runContext.metrics();
        assertThat(metrics.stream().map(AbstractMetricEntry::getName).collect(Collectors.toSet()), hasItems(
            "processed.rows", "processed.bytes", "peak.memory.bytes", "physical.written.bytes",
            "cpu.time", "wall.time", "queued.time"
        ));

        Counter processedRows = metrics.stream()
            .filter(metric -> metric.getName().equals("processed.rows"))
            .map(metric -> (Counter) metric)
            .findFirst()
            .orElseThrow();
        assertThat(processedRows.getValue().doubleValue(), greaterThan(0d));

        Timer cpuTime = metrics.stream()
            .filter(metric -> metric.getName().equals("cpu.time"))
            .map(metric -> (Timer) metric)
            .findFirst()
            .orElseThrow();
        assertThat(cpuTime.getValue(), greaterThan(Duration.ofMillis(0)));
    }

    @Test
    void typedSessionAndClientPropertiesReachTheEngine() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        Queries task = Queries.builder()
            .url(Property.ofValue(JDBC_URL))
            .username(Property.ofValue("fake"))
            .source(Property.ofValue("kestra_test"))
            .sessionProperties(Property.ofValue(Map.of("query_max_execution_time", "10m")))
            .sql(Property.ofValue("SHOW SESSION"))
            .fetchType(Property.ofValue(FETCH))
            .build();

        AbstractJdbcQueries.MultiQueryOutput output = task.run(runContext);

        var queried = output.getOutputs().getFirst().getRows()
            .stream()
            .filter(row -> String.valueOf(row.get("Name")).equals("query_max_execution_time"))
            .toList();
        assertThat(queried, hasSize(1));
        assertThat(queried.getFirst().get("Value"), is("10m"));
    }

    @Test
    void aPropertyPassedBothByTheUrlAndAsTaskPropertyFailsEarly() {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        var task = Queries.builder()
            .url(Property.ofValue("jdbc:trino://localhost:48080/memory/default?catalog=other"))
            .username(Property.ofValue("fake"))
            .catalog(Property.ofValue("memory"))
            .sql(Property.ofValue("SELECT 1"))
            .build();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> task.connectionProperties(runContext));
        assertThat(exception.getMessage(), containsString("catalog"));
    }

    @Test
    void sourceInUrlRemainsCompatibleAndDefaultsOtherwise() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        var fromUrl = Query.builder()
            .url(Property.ofValue("jdbc:trino://localhost:48080?source=legacy"))
            .build().connectionProperties(runContext);
        assertThat(fromUrl.containsKey("source"), is(false));

        var defaultSource = Query.builder().url(Property.ofValue(JDBC_URL)).build()
            .connectionProperties(runContext);
        assertThat(defaultSource.getProperty("source"), is("kestra"));

        var conflict = Query.builder()
            .url(Property.ofValue("jdbc:trino://localhost:48080?source=legacy"))
            .source(Property.ofValue("explicit"))
            .build();
        assertThrows(IllegalArgumentException.class, () -> conflict.connectionProperties(runContext));
    }

    @Test
    void typedConnectionPropertiesAreRendered() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        var properties = Query.builder()
            .url(Property.ofValue("jdbc:trino://localhost:48080"))
            .accessToken(Property.ofValue("token"))
            .catalog(Property.ofValue("memory"))
            .schema(Property.ofValue("default"))
            .ssl(Property.ofValue(false))
            .sslVerification(Property.ofValue(SslVerification.CA))
            .kerberosPrincipal(Property.ofValue("user@example.com"))
            .kerberosRemoteServiceName(Property.ofValue("trino"))
            .sessionProperties(Property.ofValue(Map.of("query_max_execution_time", "10m")))
            .clientTags(Property.ofValue(java.util.List.of("kestra", "test")))
            .roles(Property.ofValue(Map.of("memory", "admin")))
            .extraCredentials(Property.ofValue(Map.of("key", "value")))
            .build().connectionProperties(runContext);
        assertThat(properties.getProperty("accessToken"), is("token"));
        assertThat(properties.getProperty("catalog"), is("memory"));
        assertThat(properties.getProperty("schema"), is("default"));
        assertThat(properties.getProperty("SSL"), is("false"));
        assertThat(properties.getProperty("SSLVerification"), is("CA"));
        assertThat(properties.getProperty("KerberosPrincipal"), is("user@example.com"));
        assertThat(properties.getProperty("KerberosRemoteServiceName"), is("trino"));
        assertThat(properties.getProperty("sessionProperties"), is("query_max_execution_time:10m"));
        assertThat(properties.getProperty("clientTags"), is("kestra,test"));
        assertThat(properties.getProperty("roles"), is("memory:admin"));
        assertThat(properties.getProperty("extraCredentials"), is("key:value"));
    }

    @Test
    void queriesRunOutOfTransactionByDefault() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        Queries task = Queries.builder()
            .url(Property.ofValue(JDBC_URL))
            .username(Property.ofValue("fake"))
            .sql(Property.ofValue("""
                DROP TABLE IF EXISTS memory.default.trino_queries_test;
                CREATE TABLE memory.default.trino_queries_test (id INTEGER);
                INSERT INTO memory.default.trino_queries_test VALUES (1), (2);
                SELECT COUNT(*) AS rows_count FROM memory.default.trino_queries_test
                """))
            .fetchType(Property.ofValue(FETCH))
            .build();

        AbstractJdbcQueries.MultiQueryOutput output = task.run(runContext);

        assertThat(output.getStatementCount(), is(4L));
        assertThat(output.getOutputs(), hasSize(4));
        output.getOutputs().forEach(item -> assertThat(item.getQueryId(), notNullValue()));
        assertThat(output.getOutputs().getLast().getRows().getFirst().get("rows_count"), is(2L));
    }

    @Test
    void transactionalWritesAreRejectedOnTransactionlessCatalogs() {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        Queries task = Queries.builder()
            .url(Property.ofValue(JDBC_URL))
            .username(Property.ofValue("fake"))
            .transaction(Property.ofValue(true))
            .sql(Property.ofValue("""
                CREATE TABLE memory.default.trino_queries_tx (id INTEGER);
                INSERT INTO memory.default.trino_queries_tx VALUES (1);
                SELECT COUNT(*) AS rows_count FROM memory.default.trino_queries_tx
                """))
            .fetchType(Property.ofValue(FETCH_ONE))
            .build();

        // The memory catalog only accepts autocommit writes, so `transaction: true` must fail, and the run
        // of the task reports it instead of partially applying the statements.
        assertThrows(RuntimeException.class, () -> task.run(runContext));
    }
}
