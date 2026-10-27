package io.kestra.plugin.jdbc;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class JdbcAssetsTest {
    private static Connection connection;

    @BeforeAll
    static void setUp() throws Exception {
        connection = DriverManager.getConnection("jdbc:h2:mem:assets;DB_CLOSE_DELAY=-1");
        try (var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE orders (id INT NOT NULL, amount INT)");
            statement.execute("CREATE TABLE customers (id INT)");
            statement.execute("CREATE TABLE daily (total INT)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        connection.close();
    }

    @Test
    void insertSelectJoinHasInputsAndOutputWithColumns() throws Exception {
        var emit = JdbcAssets.build(
            connection,
            "INSERT INTO daily (total) SELECT sum(o.amount) FROM orders o JOIN customers c ON c.id = o.id",
            1L,
            true
        );

        assertThat(emit.inputs().stream().map(i -> i.id()).toList(), containsInAnyOrder(endsWith(".ORDERS"), endsWith(".CUSTOMERS")));
        assertThat(emit.outputs(), hasSize(1));
        var output = emit.outputs().getFirst();
        assertThat(output.getType(), is(JdbcAssets.TABLE_TYPE));
        assertThat(output.getId(), endsWith(".DAILY"));
        assertThat(output.getMetadata().get("name"), is("DAILY"));
        assertThat(output.getMetadata().get("lastStatementKind"), is("INSERT"));
        assertThat(output.getMetadata().get("rowsAffected"), is(1L));
        @SuppressWarnings("unchecked")
        var columns = (List<Map<String, Object>>) output.getMetadata().get("columns");
        assertThat(columns, hasSize(1));
        assertThat(columns.getFirst().get("name"), is("TOTAL"));
        assertThat(columns.getFirst().get("nullable"), is(true));
    }

    @Test
    void createTableAsReadsSourceAndCreatesTarget() throws Exception {
        var emit = JdbcAssets.build(connection, "CREATE TABLE copy_of_orders AS SELECT * FROM orders", 0L, true);

        assertThat(emit.outputs().getFirst().getId(), endsWith(".COPY_OF_ORDERS"));
        assertThat(emit.inputs().getFirst().id(), endsWith(".ORDERS"));
    }

    @Test
    void mergeHasTargetAndSource() throws Exception {
        var emit = JdbcAssets.build(
            connection,
            "MERGE INTO daily d USING orders o ON d.total = o.amount WHEN MATCHED THEN UPDATE SET total = o.amount",
            null,
            true
        );

        assertThat(emit.outputs().getFirst().getId(), endsWith(".DAILY"));
        assertThat(emit.inputs().getFirst().id(), endsWith(".ORDERS"));
    }

    @Test
    void selectOnSystemSchemaEmitsNothing() throws Exception {
        assertThat(JdbcAssets.build(connection, "SELECT * FROM information_schema.tables", null, true), nullValue());
    }

    @Test
    void withoutEnableAutoOnlyCreateTableIsEmittedAsOutput() throws Exception {
        var insert = JdbcAssets.build(connection, "INSERT INTO daily (total) SELECT amount FROM orders", 1L, false);
        assertThat(insert, nullValue());

        var create = JdbcAssets.build(connection, "CREATE TABLE only_created AS SELECT * FROM orders", 0L, false);
        assertThat(create.inputs(), is(empty()));
        assertThat(create.outputs(), hasSize(1));
        assertThat(create.outputs().getFirst().getMetadata().containsKey("columns"), is(false));
    }

    @Test
    void dropEmitsNothing() throws Exception {
        assertThat(JdbcAssets.build(connection, "DROP TABLE IF EXISTS gone", null, true), nullValue());
    }

    @Test
    void sameTableGetsTheSameIdWhateverTheSpelling() throws Exception {
        var ids = List.of("SELECT * FROM orders", "SELECT * FROM ORDERS", "SELECT * FROM public.orders", "SELECT * FROM \"ORDERS\"").stream()
            .map(sql -> {
                try {
                    return JdbcAssets.build(connection, sql, null, true).inputs().getFirst().id();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            })
            .collect(java.util.stream.Collectors.toSet());

        assertThat(ids, hasSize(1));
        assertThat(ids.iterator().next(), endsWith(".PUBLIC.ORDERS"));
    }

    @Test
    void quotedNamesKeepTheirCase() throws Exception {
        var emit = JdbcAssets.build(connection, "SELECT * FROM \"orders\"", null, true);

        assertThat(emit.inputs().getFirst().id(), endsWith(".PUBLIC.orders"));
    }

    @Test
    void temporaryTablesAreNotAssets() throws Exception {
        assertThat(JdbcAssets.build(connection, "CREATE TEMP TABLE scratch AS SELECT * FROM orders", 0L, false), nullValue());

        var emit = JdbcAssets.build(connection, "CREATE TEMPORARY TABLE scratch AS SELECT * FROM orders", 0L, true);
        assertThat(emit.outputs(), is(empty()));
        assertThat(emit.inputs(), hasSize(1));
    }

    @Test
    void severalStatementsGiveOneEmitWithoutTemporaryTables() throws Exception {
        var emit = JdbcAssets.build(
            connection,
            List.of(
                new JdbcAssets.ExecutedStatement("CREATE TEMP TABLE scratch AS SELECT * FROM orders", 0L),
                new JdbcAssets.ExecutedStatement("INSERT INTO daily (total) SELECT sum(amount) FROM scratch", 1L),
                new JdbcAssets.ExecutedStatement("INSERT INTO daily (total) SELECT count(*) FROM customers", 2L)
            ),
            true
        );

        assertThat(emit.outputs().stream().map(o -> o.getId()).toList(), contains(endsWith(".PUBLIC.DAILY")));
        assertThat(emit.outputs().getFirst().getMetadata().get("rowsAffected"), is(2L));
        assertThat(emit.inputs().stream().map(i -> i.id()).toList(), containsInAnyOrder(endsWith(".PUBLIC.ORDERS"), endsWith(".PUBLIC.CUSTOMERS")));
    }
}
