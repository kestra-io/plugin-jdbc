package io.kestra.plugin.jdbc;

import io.kestra.plugin.jdbc.SqlTableExtractor.Access;
import io.kestra.plugin.jdbc.SqlTableExtractor.Dialect;
import io.kestra.plugin.jdbc.SqlTableExtractor.Kind;
import io.kestra.plugin.jdbc.SqlTableExtractor.TableReference;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class SqlTableExtractorTest {

    private static List<String> summary(List<TableReference> tables) {
        return tables.stream()
            .map(t -> t.getAccess() + ":" + (t.getDatabase() == null ? "" : t.getDatabase() + ".")
                + (t.getSchema() == null ? "" : t.getSchema() + ".") + t.getName())
            .toList();
    }

    @Test
    void selectFromAndJoin() {
        var tables = SqlTableExtractor.extract(
            "SELECT * FROM public.orders o JOIN customers c ON c.id = o.customer_id LEFT JOIN analytics.items i USING (id)",
            Dialect.ANSI
        );

        assertThat(summary(tables), contains("READ:public.orders", "READ:customers", "READ:analytics.items"));
    }

    @Test
    void commaSeparatedFromList() {
        var tables = SqlTableExtractor.extract("SELECT * FROM a x, b y WHERE x.id = y.id", Dialect.ANSI);

        assertThat(summary(tables), contains("READ:a", "READ:b"));
    }

    @Test
    void insertIntoSelect() {
        var tables = SqlTableExtractor.extract(
            "INSERT INTO analytics.orders_daily (day, total) SELECT date_trunc('day', created_at), sum(amount) FROM public.orders GROUP BY 1",
            Dialect.ANSI
        );

        assertThat(summary(tables), contains("WRITE:analytics.orders_daily", "READ:public.orders"));
    }

    @Test
    void update() {
        var tables = SqlTableExtractor.extract("UPDATE sales.orders SET status = 'x' WHERE id = 1", Dialect.ANSI);

        assertThat(summary(tables), contains("WRITE:sales.orders"));
    }

    @Test
    void deleteFrom() {
        var tables = SqlTableExtractor.extract("DELETE FROM sales.orders WHERE id = 1", Dialect.ANSI);

        assertThat(summary(tables), contains("WRITE:sales.orders"));
    }

    @Test
    void mergeInto() {
        var tables = SqlTableExtractor.extract(
            "MERGE INTO dw.target t USING staging.source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)",
            Dialect.ANSI
        );

        assertThat(summary(tables), contains("WRITE:dw.target", "READ:staging.source"));
    }

    @Test
    void createTableAs() {
        var tables = SqlTableExtractor.extract("CREATE TABLE IF NOT EXISTS reports.daily AS SELECT * FROM raw.events", Dialect.ANSI);

        assertThat(summary(tables), contains("CREATE:reports.daily", "READ:raw.events"));
    }

    @Test
    void temporaryTableHasLowConfidence() {
        var tables = SqlTableExtractor.extract("CREATE TEMP TABLE scratch (id int)", Dialect.ANSI);

        assertThat(tables, hasSize(1));
        assertThat(tables.getFirst().getConfidence(), is(SqlTableExtractor.Confidence.LOW));
    }

    @Test
    void dropTable() {
        var tables = SqlTableExtractor.extract("DROP TABLE IF EXISTS old.t1, old.t2", Dialect.ANSI);

        assertThat(summary(tables), contains("DROP:old.t1", "DROP:old.t2"));
    }

    @Test
    void truncate() {
        var tables = SqlTableExtractor.extract("TRUNCATE TABLE logs.events", Dialect.ANSI);

        assertThat(summary(tables), contains("WRITE:logs.events"));
    }

    @Test
    void quotedIdentifiers() {
        var tables = SqlTableExtractor.extract("SELECT * FROM \"My Schema\".\"Order Items\"", Dialect.ANSI);

        assertThat(tables.getFirst().getSchema(), is("My Schema"));
        assertThat(tables.getFirst().getName(), is("Order Items"));
    }

    @Test
    void threePartName() {
        var tables = SqlTableExtractor.extract("SELECT * FROM db1.sch1.tbl1", Dialect.ANSI);

        assertThat(tables.getFirst().getDatabase(), is("db1"));
        assertThat(tables.getFirst().getSchema(), is("sch1"));
        assertThat(tables.getFirst().getName(), is("tbl1"));
    }

    @Test
    void cteNamesAreExcluded() {
        var tables = SqlTableExtractor.extract(
            "WITH recent AS (SELECT * FROM events WHERE ts > now()), agg AS (SELECT count(*) c FROM recent) SELECT * FROM agg JOIN users u ON true",
            Dialect.ANSI
        );

        assertThat(summary(tables), contains("READ:events", "READ:users"));
    }

    @Test
    void stringsAndCommentsAreIgnored() {
        var tables = SqlTableExtractor.extract(
            "-- FROM ghost\nSELECT 'FROM fake', /* JOIN nothing */ x FROM real_table",
            Dialect.ANSI
        );

        assertThat(summary(tables), contains("READ:real_table"));
    }

    @Test
    void postgresExtractFromIsNotATable() {
        var tables = SqlTableExtractor.extract(
            "SELECT extract(epoch FROM created_at) FROM orders ON CONFLICT DO NOTHING; INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 2",
            Dialect.ANSI
        );

        assertThat(summary(tables), contains("READ:orders", "WRITE:t"));
    }

    @Test
    void postgresDollarQuotedBodyIsIgnored() {
        var tables = SqlTableExtractor.extract(
            "CREATE TABLE audit.log AS SELECT $$ FROM ghost $$ AS body FROM source_table",
            Dialect.ANSI
        );

        assertThat(summary(tables), contains("CREATE:audit.log", "READ:source_table"));
    }

    @Test
    void mysqlBacktickAndBackslashEscape() {
        var tables = SqlTableExtractor.extract(
            "INSERT IGNORE INTO `shop`.`orders` SELECT 'it\\'s FROM ghost' FROM `shop`.`staging`",
            Dialect.MYSQL
        );

        assertThat(summary(tables), contains("WRITE:shop.orders", "READ:shop.staging"));
    }

    @Test
    void sqlServerBracketsAndTempTable() {
        var tables = SqlTableExtractor.extract(
            "SELECT * INTO #tmp FROM [sales].[dbo].[Order Lines] ol JOIN [Other DB]..[Customers] c ON 1=1",
            Dialect.SQLSERVER
        );

        assertThat(tables.getFirst().getName(), is("#tmp"));
        assertThat(tables.getFirst().getConfidence(), is(SqlTableExtractor.Confidence.LOW));
        assertThat(tables.get(1).getDatabase(), is("sales"));
        assertThat(tables.get(1).getSchema(), is("dbo"));
        assertThat(tables.get(1).getName(), is("Order Lines"));
        assertThat(tables.get(2).getDatabase(), is("Other DB"));
        assertThat(tables.get(2).getSchema(), nullValue());
        assertThat(tables.get(2).getName(), is("Customers"));
    }

    @Test
    void oracleDualAndMerge() {
        var tables = SqlTableExtractor.extract(
            "MERGE INTO hr.emp e USING (SELECT 1 id FROM dual) s ON (e.id = s.id) WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)",
            Dialect.ANSI
        );

        assertThat(summary(tables), contains("WRITE:hr.emp"));
    }

    @Test
    void snowflakeCopyIntoIgnoresStage() {
        var tables = SqlTableExtractor.extract(
            "COPY INTO analytics.public.events FROM @my_stage/path FILE_FORMAT = (TYPE = CSV)",
            Dialect.ANSI
        );

        assertThat(summary(tables), contains("WRITE:analytics.public.events"));
    }

    @Test
    void clickhouseBacktickAndTableKeyword() {
        var tables = SqlTableExtractor.extract(
            "INSERT INTO TABLE `db`.`events` SELECT * FROM `db`.`raw_events` FINAL",
            Dialect.MYSQL
        );

        assertThat(summary(tables), contains("WRITE:db.events", "READ:db.raw_events"));
    }

    @Test
    void duckdbFilesAndUrls() {
        var tables = SqlTableExtractor.extract(
            "SELECT * FROM read_parquet('/data/in.parquet') p JOIN 'https://example.com/ref.csv' r ON true JOIN local_table l ON true",
            Dialect.DUCKDB
        );

        assertThat(tables, hasSize(3));
        assertThat(tables.getFirst().getKind(), is(Kind.FILE));
        assertThat(tables.getFirst().getName(), is("/data/in.parquet"));
        assertThat(tables.get(1).getKind(), is(Kind.URL));
        assertThat(tables.get(2).getKind(), is(Kind.TABLE));
        assertThat(tables.get(2).getAccess(), is(Access.READ));
    }

    @Test
    void duckdbAttach() {
        var tables = SqlTableExtractor.extract("ATTACH '/tmp/other.db' AS other", Dialect.DUCKDB);

        assertThat(tables, hasSize(1));
        assertThat(tables.getFirst().getKind(), is(Kind.FILE));
    }

    @Test
    void emptyAndNullSql() {
        assertThat(SqlTableExtractor.extract(null, Dialect.ANSI), is(empty()));
        assertThat(SqlTableExtractor.extract("  ", Dialect.ANSI), is(empty()));
    }

    @Test
    void statementKind() {
        assertThat(SqlTableExtractor.statementKind("select 1"), is("SELECT"));
        assertThat(SqlTableExtractor.statementKind("  /* c */ INSERT INTO t VALUES (1)"), is("INSERT"));
        assertThat(SqlTableExtractor.statementKind("WITH x AS (SELECT 1) UPDATE t SET a = 1"), is("UPDATE"));
        assertThat(SqlTableExtractor.statementKind("VACUUM"), is("OTHER"));
    }

    @Test
    void duckdbUrlsNeverKeepCredentials() {
        var tables = SqlTableExtractor.extract(
            "SELECT * FROM read_csv('https://user:token@example.com/data/f.csv?password=hunter2&x=1') a "
                + "JOIN read_parquet('s3://bucket/key.parquet?X-Amz-Signature=abc&X-Amz-Credential=def') b ON true",
            Dialect.DUCKDB
        );

        assertThat(tables.stream().map(TableReference::getName).toList(), contains("https://example.com/data/f.csv", "s3://bucket/key.parquet"));
        assertThat(tables.stream().map(TableReference::getKind).toList(), contains(Kind.URL, Kind.URL));
    }

    @Test
    void duckdbAttachConnectionStringsAreNotOutput() {
        assertThat(SqlTableExtractor.extract("ATTACH 'host=pg user=app password=s3cr3t dbname=x' AS pg (TYPE postgres)", Dialect.DUCKDB), is(empty()));
        assertThat(SqlTableExtractor.extract("ATTACH 'postgres:dbname=x' AS pg", Dialect.DUCKDB), is(empty()));
        assertThat(SqlTableExtractor.extract("ATTACH 'https://user:pw@host/db.duckdb' AS r", Dialect.DUCKDB).getFirst().getName(), is("https://host/db.duckdb"));
    }
}
