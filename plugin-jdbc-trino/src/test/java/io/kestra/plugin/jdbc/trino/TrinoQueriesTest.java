package io.kestra.plugin.jdbc.trino;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.AbstractMetricEntry;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static io.kestra.core.models.tasks.common.FetchType.FETCH;
import static io.kestra.core.models.tasks.common.FetchType.FETCH_ONE;
import static io.kestra.core.models.tasks.common.FetchType.NONE;
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
        var runContext = runContextFactory.of(ImmutableMap.of());
        var task = Query.builder()
            .url(Property.ofValue(JDBC_URL))
            .username(Property.ofValue("fake"))
            .sql(Property.ofValue("SELECT 42 AS answer"))
            .fetchType(Property.ofValue(FETCH_ONE))
            .build();

        var output = task.run(runContext);

        assertThat(output.getQueryId(), notNullValue());
        assertThat(output.getQueryId(), containsString("_"));
    }

    @Test
    void queryIdIsExposedOnEveryQueriesOutput() throws Exception {
        var runContext = runContextFactory.of(ImmutableMap.of());
        var task = Queries.builder()
            .url(Property.ofValue(JDBC_URL))
            .username(Property.ofValue("fake"))
            .sql(Property.ofValue("SELECT 1 AS a; SELECT 2 AS b"))
            .fetchType(Property.ofValue(FETCH))
            .build();

        var output = task.run(runContext);

        assertThat(output.getOutputs(), hasSize(2));
        assertThat(output.getOutputs().getFirst().getQueryId(), notNullValue());
        assertThat(output.getOutputs().get(1).getQueryId(), notNullValue());
        assertThat(output.getOutputs().get(1).getRows().getFirst().get("b"), is(2));
    }

    @Test
    void queryStatsMetricsAreEmitted() throws Exception {
        var runContext = runContextFactory.of(ImmutableMap.of());
        var task = Query.builder()
            .url(Property.ofValue(JDBC_URL))
            .username(Property.ofValue("fake"))
            .sql(Property.ofValue("SELECT * FROM tpch.tiny.orders"))
            .fetchType(Property.ofValue(FETCH))
            .build();

        var output = task.run(runContext);

        assertThat(output.getQueryId(), notNullValue());

        var metrics = runContext.metrics();
        assertThat(
            metrics.stream().map(AbstractMetricEntry::getName).collect(Collectors.toSet()), hasItems(
                "processed.rows", "processed.bytes", "peak.memory.bytes", "physical.written.bytes",
                "cpu.time", "wall.time", "queued.time"
            )
        );

        var processedRows = metrics.stream()
            .filter(metric -> metric.getName().equals("processed.rows"))
            .map(metric -> (Counter) metric)
            .findFirst()
            .orElseThrow(() -> new AssertionError("processed.rows metric missing"));
        assertThat(processedRows.getValue().doubleValue(), greaterThan(0d));

        var cpuTime = metrics.stream()
            .filter(metric -> metric.getName().equals("cpu.time"))
            .map(metric -> (Timer) metric)
            .findFirst()
            .orElseThrow(() -> new AssertionError("cpu.time metric missing"));
        assertThat(cpuTime.getValue(), greaterThan(Duration.ofMillis(0)));
    }

    @Test
    void typedSessionAndClientPropertiesReachTheEngine() throws Exception {
        var runContext = runContextFactory.of(ImmutableMap.of());
        var task = Queries.builder()
            .url(Property.ofValue(JDBC_URL))
            .username(Property.ofValue("fake"))
            .source(Property.ofValue("kestra_test"))
            .sessionProperties(Property.ofValue(Map.of("query_max_execution_time", "10m")))
            .sql(Property.ofValue("SHOW SESSION"))
            .fetchType(Property.ofValue(FETCH))
            .build();

        var output = task.run(runContext);

        var queried = output.getOutputs().getFirst().getRows()
            .stream()
            .filter(row -> String.valueOf(row.get("Name")).equals("query_max_execution_time"))
            .toList();
        assertThat(queried, hasSize(1));
        assertThat(queried.getFirst().get("Value"), is("10m"));
    }

    @Test
    void sourceInUrlRemainsCompatibleAndDefaultsOtherwise() throws Exception {
        var runContext = runContextFactory.of(ImmutableMap.of());
        var fromUrl = Query.builder()
            .url(Property.ofValue("jdbc:trino://localhost:48080?source=legacy"))
            .build().connectionProperties(runContext);
        assertThat(fromUrl.containsKey("source"), is(false));

        var defaultSource = Query.builder().url(Property.ofValue(JDBC_URL)).build()
            .connectionProperties(runContext);
        assertThat(defaultSource.getProperty("source"), is("kestra"));
    }

    @Test
    void propertySetInUrlAndAsTaskPropertyFailsWithFixHint() {
        var runContext = runContextFactory.of(ImmutableMap.of());
        var task = Query.builder()
            .url(Property.ofValue("jdbc:trino://localhost:48080/memory?source=legacy"))
            .username(Property.ofValue("fake"))
            .source(Property.ofValue("explicit"))
            .sql(Property.ofValue("SELECT 1"))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), is("The `source` connection property is set both in the JDBC URL and as a task property. Remove one of the two."));
    }

    @Test
    void credentialFilesAreMaterializedFromBase64TextAndStorage() throws Exception {
        var context = runContextFactory.of(ImmutableMap.of());
        byte[] keyBytes = new byte[] { 0, 1, 2, (byte) 255 };
        var storageFile = context.workingDir().createTempFile(".keytab");
        Files.write(storageFile, new byte[] { 4, 5, 6 });
        var uri = context.storage().putFile(storageFile.toFile());
        var properties = Query.builder()
            .url(Property.ofValue(JDBC_URL))
            .sslKeyStore(Property.ofValue(Base64.getEncoder().encodeToString(keyBytes)))
            .sslKeyStorePassword(Property.ofValue("password"))
            .sslKeyStoreType(Property.ofValue("JKS"))
            .sslTrustStore(Property.ofValue("-----BEGIN CERTIFICATE-----\ntext\n-----END CERTIFICATE-----"))
            .kerberosKeytab(Property.ofValue(uri.toString()))
            .build().connectionProperties(context);
        Assertions.assertArrayEquals(keyBytes, Files.readAllBytes(Path.of(properties.getProperty("SSLKeyStorePath"))));
        assertThat(properties.getProperty("SSLKeyStorePassword"), is("password"));
        assertThat(properties.getProperty("SSLKeyStoreType"), is("JKS"));
        assertThat(Files.readString(Path.of(properties.getProperty("SSLTrustStorePath"))), containsString("BEGIN CERTIFICATE"));
        Assertions.assertArrayEquals(new byte[] { 4, 5, 6 }, Files.readAllBytes(Path.of(properties.getProperty("KerberosKeytabPath"))));
    }

    @Test
    void noneFetchStillEmitsStatsForQueryAndQueries() throws Exception {
        var single = runContextFactory.of(ImmutableMap.of());
        Query.builder().url(Property.ofValue(JDBC_URL)).username(Property.ofValue("fake"))
            .sql(Property.ofValue("SELECT * FROM tpch.tiny.orders"))
            .fetchType(Property.ofValue(NONE)).build().run(single);
        assertThat(single.metrics().stream().anyMatch(m -> m.getName().equals("processed.rows")), is(true));

        var multi = runContextFactory.of(ImmutableMap.of());
        Queries.builder().url(Property.ofValue(JDBC_URL)).username(Property.ofValue("fake"))
            .sql(Property.ofValue("SELECT * FROM tpch.tiny.orders; SELECT * FROM tpch.tiny.orders"))
            .fetchType(Property.ofValue(NONE)).build().run(multi);
        assertThat(multi.metrics().stream().anyMatch(m -> m.getName().equals("processed.rows")), is(true));
    }

    @Test
    void typedConnectionPropertiesAreRendered() throws Exception {
        var runContext = runContextFactory.of(ImmutableMap.of());
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
            .clientTags(Property.ofValue(List.of("kestra", "test")))
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
        var runContext = runContextFactory.of(ImmutableMap.of());
        var task = Queries.builder()
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

        var output = task.run(runContext);

        assertThat(output.getStatementCount(), is(4L));
        assertThat(output.getOutputs(), hasSize(1));
        output.getOutputs().forEach(item -> assertThat(item.getQueryId(), notNullValue()));
        assertThat(output.getOutputs().getLast().getRows().getFirst().get("rows_count"), is(2L));
    }

    @Test
    void transactionalWritesAreRejectedOnTransactionlessCatalogs() throws Exception {
        var runContext = runContextFactory.of(ImmutableMap.of());
        Queries.builder()
            .url(Property.ofValue(JDBC_URL))
            .username(Property.ofValue("fake"))
            .transaction(Property.ofValue(false))
            .sql(Property.ofValue("DROP TABLE IF EXISTS memory.default.trino_queries_tx"))
            .build()
            .run(runContext);

        var task = Queries.builder()
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

        // The memory catalog only accepts autocommit writes, so `transaction: true` must fail.
        var exception = assertThrows(RuntimeException.class, () -> task.run(runContext));
        assertThat(exception.getCause().getMessage(), containsString("autocommit"));
    }
}
