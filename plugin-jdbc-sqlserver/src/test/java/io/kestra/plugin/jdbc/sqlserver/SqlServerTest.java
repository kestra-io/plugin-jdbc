package io.kestra.plugin.jdbc.sqlserver;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.time.*;

import static io.kestra.core.models.tasks.common.FetchType.FETCH_ONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * See :
 * - https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15
 */
@KestraTest
public class SqlServerTest extends AbstractRdbmsTest {
    @Test
    void select() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchType(FETCH_ONE)
            .timeZoneId("Europe/Paris")
            .sql("select * from sqlserver_types")
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        String hour = System.getenv("GITHUB_WORKFLOW") != null ? "14" : "12";

        assertThat(runOutput.getRow().get("t_null"), is(nullValue()));
        assertThat(runOutput.getRow().get("t_bigint"), is(9223372036854775807L));
        assertThat(runOutput.getRow().get("t_int"), is(2147483647));
        assertThat(runOutput.getRow().get("t_smallint"), is((short)32767));
        assertThat(runOutput.getRow().get("t_tinyint"), is((short)255));
        assertThat(runOutput.getRow().get("t_float"), is(12345.12345D));
        assertThat(runOutput.getRow().get("t_real"), is(12345.12345F));
        assertThat(runOutput.getRow().get("t_decimal"), is(BigDecimal.valueOf(123.46)));
        assertThat(runOutput.getRow().get("t_numeric"), is(BigDecimal.valueOf(12345.12345)));
        assertThat(runOutput.getRow().get("t_bit"), is(true));
        assertThat(runOutput.getRow().get("t_smallmoney"), is(BigDecimal.valueOf(3148.2929)));
        assertThat(runOutput.getRow().get("t_money"), is(BigDecimal.valueOf(3148.1234)));
        assertThat(runOutput.getRow().get("t_char") , is("test      "));
        assertThat(runOutput.getRow().get("t_varchar"), is("test"));
        assertThat(runOutput.getRow().get("t_nchar"), is("test      "));
        assertThat(runOutput.getRow().get("t_nvarchar"), is("test"));
        assertThat(runOutput.getRow().get("t_text"), is("test"));
        assertThat(runOutput.getRow().get("t_ntext"), is("test"));
        assertThat(runOutput.getRow().get("t_time"), is(LocalTime.parse("12:35:29")));
        assertThat(runOutput.getRow().get("t_date"), is(LocalDate.parse("2007-05-08")));
        assertThat(runOutput.getRow().get("t_smalldatetime"), is(ZonedDateTime.parse("2007-05-08T" + hour + ":35+02:00[Europe/Paris]")));
        assertThat(runOutput.getRow().get("t_datetime"), is(ZonedDateTime.parse("2007-05-08T" + hour + ":35:29.123+02:00[Europe/Paris]")));
        assertThat(runOutput.getRow().get("t_datetime2"), is(ZonedDateTime.parse("2007-05-08T" + hour + ":35:29.123456700+02:00[Europe/Paris]")));
        assertThat(runOutput.getRow().get("t_datetimeoffset"), is(OffsetDateTime.parse("2007-05-08T12:35:29.123456700+12:15")));
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
            .sql("UPDATE sqlserver_types SET t_varchar = 'D' WHERE t_varchar = 'test'")
            .build();

        taskUpdate.run(runContext);


        Query taskGet = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchType(FETCH_ONE)
            .timeZoneId("Europe/Paris")
            .sql("select t_varchar from sqlserver_types")
            .build();

        AbstractJdbcQuery.Output runOutput = taskGet.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());
        assertThat(runOutput.getRow().get("t_varchar"), is("D"));
    }

    @Override
    protected String getUrl() {
        return "jdbc:sqlserver://localhost:41433;trustServerCertificate=true";
    }

    @Override
    protected String getUsername() {
        return "sa";
    }

    @Override
    protected String getPassword() {
        return "Sqls3rv3r_Pa55word!";
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/sqlserver.sql");
    }
}
