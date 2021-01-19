package org.kestra.task.jdbc.clickhouse;

import com.google.common.collect.ImmutableMap;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.kestra.core.runners.RunContext;
import org.kestra.task.jdbc.AbstractJdbcQuery;
import org.kestra.task.jdbc.AbstractRdbmsTest;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
public class ClickHouseTest extends AbstractRdbmsTest {
    @Test
    void select() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchOne(true)
            .timeZoneId("Europe/Paris")
            .sql("select * from clickhouse_types")
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());
        
        assertThat(runOutput.getRow().get("Int8"), is(123));
        assertThat(runOutput.getRow().get("Float32"), is(2147483645.1234F));
        assertThat(runOutput.getRow().get("Float64"), is(2147483645.1234D));
        assertThat(runOutput.getRow().get("Decimal"), is(new BigDecimal("2147483645.1234")));
        assertThat(runOutput.getRow().get("String"), is("four"));
        assertThat(runOutput.getRow().get("FixedString"), is("four"));
        assertThat(runOutput.getRow().get("Uuid"), is(UUID.fromString("6bbf0744-74b4-46b9-bb05-53905d4538e7")));
        assertThat(runOutput.getRow().get("Date"), is(LocalDate.parse("2030-12-25")));
        assertThat(runOutput.getRow().get("DateTime"), is(LocalDateTime.parse("2004-10-19T08:23:54").atZone(ZoneId.of("Europe/Paris"))));
        assertThat(runOutput.getRow().get("DateTime64"), is(LocalDateTime.parse("2004-10-19T08:23:54.999").atZone(ZoneId.of("Europe/Paris"))));
        assertThat(runOutput.getRow().get("Enum"), is("hello"));
        assertThat(runOutput.getRow().get("LowCardinality"), is("four"));
        assertThat(runOutput.getRow().get("Array"), is(new String[]{"a", "b"}));
        assertThat(runOutput.getRow().get("Nested.NestedId"), is(new Integer[]{123}));
        assertThat(runOutput.getRow().get("Nested.NestedString"), is(new String[]{"four"}));
        assertThat(runOutput.getRow().get("Tuple"), is("('a',1)"));
        assertThat(runOutput.getRow().get("Nullable"), is(nullValue()));
        assertThat(runOutput.getRow().get("Ipv4"), is("116.253.40.133"));
        assertThat(runOutput.getRow().get("Ipv6"), is("2a02:aa08:e000:3100::2"));
    }

    @Test
    void update() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query taskUpdate = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchOne(true)
            .timeZoneId("Europe/Paris")
            .sql("ALTER TABLE clickhouse_types UPDATE String = 'D' WHERE String = 'four'")
            .build();

        taskUpdate.run(runContext);

        // clickhouse need some to refresh
        Thread.sleep(500);

        Query taskGet = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchOne(true)
            .timeZoneId("Europe/Paris")
            .sql("select String from clickhouse_types")
            .build();

        AbstractJdbcQuery.Output runOutput = taskGet.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());
        assertThat(runOutput.getRow().get("String"), is("D"));
    }

    @Override
    protected String getUrl() {
        return "jdbc:clickhouse://127.0.0.1:28123/default";
    }

    @Override
    protected String getUsername() {
        return null;
    }

    @Override
    protected String getPassword() {
        return null;
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/clickhouse.sql");
    }
}
