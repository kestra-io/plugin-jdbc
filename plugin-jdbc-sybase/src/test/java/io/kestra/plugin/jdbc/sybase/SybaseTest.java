package io.kestra.plugin.jdbc.sybase;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static io.kestra.core.models.tasks.common.FetchType.FETCH_ONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@Disabled("Lauching Sybase via our docker-compose crash the CI")
public class SybaseTest extends AbstractRdbmsTest {
    @Test
    void select() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchType(FETCH_ONE)
            .timeZoneId("Europe/Paris")
            .sql("select * from syb_types")
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get("concert_id"), is(1));

        assertThat(runOutput.getRow().get("available"), is(1));
        assertThat(runOutput.getRow().get("a"), is("four"));
        assertThat(runOutput.getRow().get("b"), is("This is a varchar"));
        assertThat(runOutput.getRow().get("c"), is("This is a text column data"));
        assertThat(runOutput.getRow().get("d"), nullValue());

        assertThat(runOutput.getRow().get("play_time"), is(-9223372036854775808L));
        assertThat(runOutput.getRow().get("library_record"), is(1844674407370955161L));
        assertThat(runOutput.getRow().get("bitn_test"), is(true));

        assertThat(runOutput.getRow().get("floatn_test"), is(9.223372F));
        assertThat(runOutput.getRow().get("double_test"), is(9.223372D));
        assertThat(runOutput.getRow().get("doublen_test"), is(2147483645.1234d));
        assertThat(runOutput.getRow().get("numeric_test"), is(new BigDecimal("5.36")));
        assertThat(runOutput.getRow().get("salary_decimal"), is(new BigDecimal("999.99")));

        assertThat(runOutput.getRow().get("date_type"), is(LocalDate.parse("2030-12-25")));
        assertThat(runOutput.getRow().get("datetime_type"), is(LocalDateTime.parse("2050-12-31T22:59:57")));
        assertThat(runOutput.getRow().get("time_type"), is(LocalTime.parse("04:05:30")));
    }

    @Test
    void update() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query taskUpdate = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchType(FETCH_ONE)
            .timeZoneId("Europe/Paris")
            .sql("update syb_types set d = 'D'")
            .build();

        taskUpdate.run(runContext);

        Query taskGet = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchType(FETCH_ONE)
            .timeZoneId("Europe/Paris")
            .sql("select d from syb_types")
            .build();

        AbstractJdbcQuery.Output runOutput = taskGet.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());
        assertThat(runOutput.getRow().get("d"), is("D"));
    }

    @Override
    protected String getUrl() {
        return "jdbc:sybase:Tds:127.0.0.1:5000/kestra";
    }

    @Override
    protected String getUsername() {
        return "sa";
    }

    @Override
    protected String getPassword() {
        return "myPassword";
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/sybase.sql");
    }

}
