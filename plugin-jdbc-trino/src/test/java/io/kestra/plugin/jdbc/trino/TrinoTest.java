package io.kestra.plugin.jdbc.trino;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.*;
import java.util.Map;

import static io.kestra.core.models.tasks.common.FetchType.FETCH_ONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * See :
 * - https://trino.io/docs/current/language/types.html?highlight=data%20type#date
 */
@KestraTest
public class TrinoTest extends AbstractRdbmsTest {
    @SuppressWarnings("unchecked")
    @Test
    void select() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(Property.of(getUrl()))
            .username(Property.of(getUsername()))
            .password(null)
            .fetchType(Property.of(FETCH_ONE))
            .timeZoneId(Property.of("Europe/Paris"))
            .sql(Property.of("select * from trino_types"))
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get("t_null"), is(nullValue()));
        assertThat(runOutput.getRow().get("t_boolean"), is(true));
        assertThat(runOutput.getRow().get("t_tinyint"), is(127));
        assertThat(runOutput.getRow().get("t_smallint"), is((short)32767));
        assertThat(runOutput.getRow().get("t_integer"), is(2147483647));
        assertThat(runOutput.getRow().get("t_bigint"), is(9223372036854775807L));
        assertThat(runOutput.getRow().get("t_real"), is(12345.124F));
        assertThat(runOutput.getRow().get("t_double"), is(12345.12345D));
        assertThat(runOutput.getRow().get("t_decimal"), is(BigDecimal.valueOf(12345600L, 5)));
        assertThat(runOutput.getRow().get("t_varchar"), is("test"));
        assertThat(runOutput.getRow().get("t_char"), is("test      "));
        assertThat(runOutput.getRow().get("t_varbinary"), is("eh?".getBytes(StandardCharsets.UTF_8)));
        assertThat((Map<String, String>) runOutput.getRow().get("t_json") , is(Map.of("a", "b")));
        assertThat(runOutput.getRow().get("t_date"), is(LocalDate.parse("2001-08-22")));
        assertThat(runOutput.getRow().get("t_time"), is(LocalTime.parse("01:02:03")));
        assertThat(runOutput.getRow().get("t_time_tz"), is(LocalTime.parse("09:02:03"))); // offset -0800
        assertThat(runOutput.getRow().get("t_timestamp"), is(LocalDateTime.parse("2020-06-10T15:55:23.383345")));
        assertThat(runOutput.getRow().get("t_timestamp_tz"), is(ZonedDateTime.parse("2001-08-22T13:04:05.321+02:00[Europe/Paris]")));
        assertThat(runOutput.getRow().get("t_interval_year"), is("P3M"));
        assertThat(runOutput.getRow().get("t_interval_day"), is("PT48H"));
        assertThat(runOutput.getRow().get("t_ipaddress"), is("10.0.0.1"));
        assertThat(runOutput.getRow().get("t_uuid"), is("12151fd2-7586-11e9-8f9e-2a86e4085a59"));

    }

    @Override
    protected String getUrl() {
        return "jdbc:trino://localhost:48080/memory/default";
    }

    @Override
    protected String getUsername() {
        return "fake";
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/trino.sql");
    }
}
