package io.kestra.plugin.jdbc.duckdb;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.schedulers.DefaultScheduler;
import io.kestra.core.schedulers.SchedulerExecutionStateInterface;
import io.kestra.core.schedulers.SchedulerTriggerStateInterface;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractJdbcTriggerTest;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@MicronautTest
class DuckDBTriggerTest extends AbstractJdbcTriggerTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        // init DB with a Query task as DuckDB has a special URL with local file in it
        Query task = Query.builder()
            .fetchOne(true)
            .timeZoneId("Europe/Paris")
            .sql("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');\n" +
                "\n" +
                "SET TimeZone='America/Los_Angeles';\n" +
                "\n" +
                "CREATE TABLE duck_types (\n" +
                "    t_null BIGINT,\n" +
                "    t_bigint BIGINT,\n" +
                "    t_boolean BOOLEAN,\n" +
                "--     t_blob BLOB,\n" +
                "    t_date DATE,\n" +
                "    t_double DOUBLE,\n" +
                "    t_decimal DECIMAL,\n" +
                "    t_hugeint HUGEINT,\n" +
                "    t_integer INTEGER,\n" +
                "    t_interval INTERVAL,\n" +
                "    t_real REAL,\n" +
                "    t_smallint SMALLINT,\n" +
                "    t_time TIME,\n" +
                "    t_timestamp TIMESTAMP,\n" +
                "    t_timestamptz TIMESTAMPTZ,\n" +
                "    t_tinyint TINYINT,\n" +
                "    t_ubigint UBIGINT,\n" +
                "    t_uinteger UINTEGER,\n" +
                "    t_usmallint USMALLINT,\n" +
                "    t_utinyint UTINYINT,\n" +
                "--     t_uuid UUID,\n" +
                "    t_varchar VARCHAR,\n" +
                "--     t_list INT[],\n" +
                "--     t_struct STRUCT(i INT, j VARCHAR),\n" +
                "--     t_map MAP(INT, VARCHAR),\n" +
                "    t_enum mood\n" +
                ");\n" +
                "\n" +
                "INSERT INTO duck_types\n" +
                "(\n" +
                "    t_null,\n" +
                "    t_bigint,\n" +
                "    t_boolean,\n" +
                "--     t_blob,\n" +
                "    t_date,\n" +
                "    t_double,\n" +
                "    t_decimal,\n" +
                "    t_hugeint,\n" +
                "    t_integer,\n" +
                "    t_interval,\n" +
                "    t_real,\n" +
                "    t_smallint,\n" +
                "    t_time,\n" +
                "    t_timestamp,\n" +
                "    t_timestamptz,\n" +
                "    t_tinyint,\n" +
                "    t_ubigint,\n" +
                "    t_uinteger,\n" +
                "    t_usmallint,\n" +
                "    t_utinyint,\n" +
                "--     t_uuid,\n" +
                "    t_varchar,\n" +
                "--     t_list,\n" +
                "--     t_struct,\n" +
                "--     t_map,\n" +
                "    t_enum\n" +
                ")\n" +
                "VALUES\n" +
                "(\n" +
                "    NULL,\n" +
                "    9223372036854775807,\n" +
                "    TRUE,\n" +
                "--     '\\xAA\\xAB\\xAC'::BLOB,\n" +
                "    DATE '1992-09-20',\n" +
                "    12345.12345,\n" +
                "    12345.12345,\n" +
                "    9223372036854775807,\n" +
                "    2147483647,\n" +
                "    INTERVAL '28' DAYS,\n" +
                "    123.456,\n" +
                "    127,\n" +
                "    TIME '01:02:03.456',\n" +
                "    TIMESTAMP '2020-06-10 15:55:23.383345',\n" +
                "    TIMESTAMPTZ '2001-08-22 03:04:05.321',\n" +
                "    127,\n" +
                "    9223372036854775807,\n" +
                "    2147483647,\n" +
                "    32767,\n" +
                "    127,\n" +
                "--     UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59',\n" +
                "    'test',\n" +
                "--     [1, 2, 3],\n" +
                "--     {'i': 42, 'j': 'a'},\n" +
                "--     map([1,2],['a','b']),\n" +
                "    'happy'\n" +
                ");\n" +
                "\n" +
                "SELECT * FROM duck_types")
            .build();

        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getSize(), is(equalTo(1L)));

        // trigger the flow
        var execution = triggerFlow(this.getClass().getClassLoader(), "flows","duckdb-listen");

        var rows = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");
        assertThat(rows.size(), is(69));
    }

    @Override
    protected String getUrl() {
        return null;
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        // do nothing as we init the database manually from the test method.
    }
}