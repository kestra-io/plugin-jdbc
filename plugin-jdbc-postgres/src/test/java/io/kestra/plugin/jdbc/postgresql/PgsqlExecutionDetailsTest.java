package io.kestra.plugin.jdbc.postgresql;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.jdbc.SqlTableExtractor;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class PgsqlExecutionDetailsTest {
    @Inject
    private RunContextFactory runContextFactory;

    private Queries.QueriesBuilder<?, ?> queries(String sql) {
        return Queries.builder()
            .url(Property.ofValue(TestUtils.url()))
            .username(Property.ofValue(TestUtils.username()))
            .password(Property.ofValue(TestUtils.password()))
            .sql(Property.ofValue(sql));
    }

    @Test
    void insertSelectJoinExposesLineageDetails() throws Exception {
        queries("""
            DROP TABLE IF EXISTS exec_orders; DROP TABLE IF EXISTS exec_customers; DROP TABLE IF EXISTS exec_daily;
            CREATE TABLE exec_orders (id int, customer_id int, amount int);
            CREATE TABLE exec_customers (id int);
            CREATE TABLE exec_daily (total int);
            INSERT INTO exec_orders VALUES (1, 1, 10), (2, 1, 20);
            INSERT INTO exec_customers VALUES (1);
            """).build().run(runContextFactory.of(Map.of()));

        var output = Query.builder()
            .url(Property.ofValue(TestUtils.url()))
            .username(Property.ofValue(TestUtils.username()))
            .password(Property.ofValue(TestUtils.password()))
            .outputStatement(Property.ofValue(true))
            .sql(Property.ofValue("INSERT INTO exec_daily (total) SELECT sum(o.amount) FROM exec_orders o JOIN exec_customers c ON c.id = o.customer_id"))
            .build()
            .run(runContextFactory.of(Map.of()));

        assertThat(output.getRowsAffected(), is(1L));
        assertThat(output.getDurationMs(), greaterThanOrEqualTo(0L));
        assertThat(output.getStatement(), startsWith("INSERT INTO exec_daily"));
        assertThat(
            output.getTables().stream().map(t -> t.getAccess() + ":" + t.getName()).toList(),
            containsInAnyOrder("WRITE:exec_daily", "READ:exec_orders", "READ:exec_customers")
        );
        assertThat(output.getTables().getFirst().getConfidence(), is(SqlTableExtractor.Confidence.MEDIUM));
    }

    @Test
    void noticesAreCountedAsWarnings() throws Exception {
        var output = Query.builder()
            .url(Property.ofValue(TestUtils.url()))
            .username(Property.ofValue(TestUtils.username()))
            .password(Property.ofValue(TestUtils.password()))
            .sql(Property.ofValue("DO $$ BEGIN RAISE NOTICE 'hello'; END $$"))
            .build()
            .run(runContextFactory.of(Map.of()));

        assertThat(output.getWarnings(), is(1));
    }

    @Test
    void credentialsInUrlNeverAppearInOutputs() throws Exception {
        var url = "jdbc:postgresql://127.0.0.1:56983/?user=" + TestUtils.username() + "&password=" + TestUtils.password();
        var output = Query.builder()
            .url(Property.ofValue(url))
            .outputStatement(Property.ofValue(true))
            .sql(Property.ofValue("SELECT 1 AS one"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build()
            .run(runContextFactory.of(Map.of()));

        var json = JacksonMapper.ofJson().writeValueAsString(output);
        assertThat(json, not(containsString(TestUtils.password())));
        assertThat(json, not(containsString("127.0.0.1")));
    }
}
