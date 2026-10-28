package io.kestra.plugin.jdbc;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JdbcAssetsTest {
    private static Connection connection;

    @BeforeAll
    static void setUp() throws Exception {
        connection = DriverManager.getConnection("jdbc:h2:mem:assets;DB_CLOSE_DELAY=-1");
        try (var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE orders (id INT NOT NULL, amount INT)");
            statement.execute("CREATE TABLE customers (id INT)");
            statement.execute("CREATE TABLE daily (total INT)");
            statement.execute("CREATE SCHEMA other");
            statement.execute("CREATE TABLE dup (b INT)");
            statement.execute("CREATE TABLE other.dup (a INT)");
            statement.execute("CREATE TABLE wild_card (x INT)");
            statement.execute("CREATE TABLE wildXcard (y INT)");
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
            1L
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
        var emit = JdbcAssets.build(connection, "CREATE TABLE copy_of_orders AS SELECT * FROM orders", 0L);

        assertThat(emit.outputs().getFirst().getId(), endsWith(".COPY_OF_ORDERS"));
        assertThat(emit.inputs().getFirst().id(), endsWith(".ORDERS"));
    }

    @Test
    void mergeHasTargetAndSource() throws Exception {
        var emit = JdbcAssets.build(
            connection,
            "MERGE INTO daily d USING orders o ON d.total = o.amount WHEN MATCHED THEN UPDATE SET total = o.amount",
            null
        );

        assertThat(emit.outputs().getFirst().getId(), endsWith(".DAILY"));
        assertThat(emit.inputs().getFirst().id(), endsWith(".ORDERS"));
    }

    @Test
    void selectOnSystemSchemaEmitsNothing() throws Exception {
        assertThat(JdbcAssets.build(connection, "SELECT * FROM information_schema.tables", null), nullValue());
    }

    @Test
    void dropEmitsNothing() throws Exception {
        assertThat(JdbcAssets.build(connection, "DROP TABLE IF EXISTS gone", null), nullValue());
    }

    @Test
    void sameTableGetsTheSameIdWhateverTheSpelling() throws Exception {
        var ids = List.of("SELECT * FROM orders", "SELECT * FROM ORDERS", "SELECT * FROM public.orders", "SELECT * FROM \"ORDERS\"").stream()
            .map(sql -> {
                try {
                    return JdbcAssets.build(connection, sql, null).inputs().getFirst().id();
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
        var emit = JdbcAssets.build(connection, "SELECT * FROM \"orders\"", null);

        assertThat(emit.inputs().getFirst().id(), endsWith(".PUBLIC.orders"));
    }

    @Test
    void temporaryTablesAreNotAssets() throws Exception {
        var emit = JdbcAssets.build(connection, "CREATE TEMPORARY TABLE scratch AS SELECT * FROM orders", 0L);
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
            null
        );

        assertThat(emit.outputs().stream().map(o -> o.getId()).toList(), contains(endsWith(".PUBLIC.DAILY")));
        assertThat(emit.outputs().getFirst().getMetadata().get("rowsAffected"), is(2L));
        assertThat(emit.inputs().stream().map(i -> i.id()).toList(), containsInAnyOrder(endsWith(".PUBLIC.ORDERS"), endsWith(".PUBLIC.CUSTOMERS")));
    }

    @Test
    void aScriptSentAsOneCallKeepsTheKindOfItsLastStatement() throws Exception {
        var emit = JdbcAssets.build(
            connection,
            "DROP TABLE IF EXISTS daily; INSERT INTO daily (total) VALUES (1); UPDATE daily SET total = 2",
            3L
        );

        assertThat(emit.outputs(), hasSize(1));
        var metadata = emit.outputs().getFirst().getMetadata();
        assertThat(metadata.get("lastStatementKind"), is("UPDATE"));
        // one call, several statements: the count is not attributed to any of them
        assertThat(metadata.get("rowsAffected"), is(nullValue()));
        assertThat(columnNames(emit.outputs().getFirst()), contains("TOTAL"));
    }

    @Test
    void unqualifiedTableOnlyGetsTheColumnsOfTheCurrentSchema() throws Exception {
        var emit = JdbcAssets.build(connection, "INSERT INTO dup (b) VALUES (1)", 1L);

        assertThat(columnNames(emit.outputs().getFirst()), contains("B"));
    }

    @Test
    void underscoreIsNotAWildcardInTheColumnLookup() throws Exception {
        var emit = JdbcAssets.build(connection, "INSERT INTO wild_card (x) VALUES (1)", 1L);

        assertThat(columnNames(emit.outputs().getFirst()), contains("X"));
    }

    @Test
    void columnsAreOnlyLookedUpForTheFirstTables() throws Exception {
        var statements = new java.util.ArrayList<JdbcAssets.ExecutedStatement>();
        for (var i = 0; i < JdbcAssets.MAX_COLUMN_LOOKUPS + 2; i++) {
            try (var statement = connection.createStatement()) {
                statement.execute("CREATE TABLE IF NOT EXISTS capped_" + i + " (total INT)");
            }
            statements.add(new JdbcAssets.ExecutedStatement("INSERT INTO capped_" + i + " (total) VALUES (1)", 1L));
        }

        var outputs = JdbcAssets.build(connection, statements, null).outputs();

        assertThat(outputs, hasSize(JdbcAssets.MAX_COLUMN_LOOKUPS + 2));
        assertThat(outputs.getFirst().getMetadata().containsKey("columns"), is(true));
        assertThat(outputs.getLast().getMetadata().containsKey("columns"), is(false));
    }

    @Test
    void catalogOnlyDriversTreatTheQualifierAsTheDatabase() throws Exception {
        var metaData = mock(DatabaseMetaData.class);
        when(metaData.getDatabaseProductName()).thenReturn("CatalogOnlyDb");
        when(metaData.supportsSchemasInDataManipulation()).thenReturn(false);
        when(metaData.supportsCatalogsInDataManipulation()).thenReturn(true);
        when(metaData.supportsMixedCaseIdentifiers()).thenReturn(true);
        when(metaData.getSearchStringEscape()).thenReturn("\\");
        var mocked = mock(Connection.class);
        when(mocked.getMetaData()).thenReturn(metaData);
        when(mocked.getCatalog()).thenReturn("kestra");

        var qualified = JdbcAssets.build(mocked, "INSERT INTO kestra.exec_t (id) VALUES (1)", 1L);
        var bare = JdbcAssets.build(mocked, "INSERT INTO exec_t (id) VALUES (1)", 1L);

        assertThat(qualified.outputs().getFirst().getId(), is("kestra.exec_t"));
        assertThat(bare.outputs().getFirst().getId(), is("kestra.exec_t"));
        assertThat(qualified.outputs().getFirst().getMetadata().containsKey("schema"), is(false));
        assertThat(qualified.outputs().getFirst().getMetadata().get("database"), is("kestra"));
    }

    @SuppressWarnings("unchecked")
    private static List<String> columnNames(io.kestra.core.models.assets.Asset asset) {
        return ((List<Map<String, Object>>) asset.getMetadata().get("columns")).stream().map(c -> (String) c.get("name")).toList();
    }
}
