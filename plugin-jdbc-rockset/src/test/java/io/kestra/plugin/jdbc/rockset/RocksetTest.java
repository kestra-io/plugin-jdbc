package io.kestra.plugin.jdbc.rockset;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.*;
import java.util.List;

import static io.kestra.core.models.tasks.common.FetchType.FETCH_ONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * See :
 * - https://rockset.io/docs/current/language/types.html?highlight=data%20type#date
 */
@KestraTest
@Requires(property = "ROCKSET_APIKEY")
@EnabledIfEnvironmentVariable(named = "ROCKSET_APIKEY", matches = ".*")
public class RocksetTest {
    @Inject
    RunContextFactory runContextFactory;

    @Property(name = "ROCKSET_APIKEY")
    String apiKey;

    @SuppressWarnings("unchecked")
    @Test
    void select() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url("jdbc:rockset://")
            .apiKey(apiKey)
            .apiServer("api.euc1a1.rockset.com")
            .fetchType(FETCH_ONE)
            .timeZoneId("Europe/Paris")
            .sql("SELECT\n" +
                "  _id,\n" +
                "  _event_time,\n" +
                "  \"null\",\n" +
                "  arrayInt,\n" +
                "  bool,\n" +
                "  bytes,\n" +
                "  date,\n" +
                "  datetime,\n" +
                "  float,\n" +
//                "  geo,\n" +
                "  int,\n" +
//                "  intervalMicro,\n" +
//                "  intervalMonth,\n" +
                "  objectInt,\n" +
                "  string,\n" +
                "  time,\n" +
                "  timestamp\n" +
                "FROM commons.\"all_types\"")
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get("null"), is(nullValue()));
        assertThat((List<Integer>) runOutput.getRow().get("arrayInt"), containsInAnyOrder(1.0D, 2.0D, 3.0D));
        assertThat(runOutput.getRow().get("bool"), is(true));
        assertThat(runOutput.getRow().get("bytes"), is("Zm9v"));
        assertThat(runOutput.getRow().get("date"), is(LocalDate.parse("2018-01-01")));
        assertThat(runOutput.getRow().get("datetime"), is(ZonedDateTime.parse("2018-01-01T10:30:45.456+01:00[Europe/Paris]")));
        assertThat(runOutput.getRow().get("float"), is(3.123456789D));
        assertThat(runOutput.getRow().get("int"), is(123456789D));
        // assertThat(runOutput.getRow().get("objectInt"), is(Map.of("a", 10D, "b", 20D)));
        assertThat(runOutput.getRow().get("string"), is("string"));
        assertThat(runOutput.getRow().get("time"), is(LocalTime.parse("09:30:45")));
        assertThat(runOutput.getRow().get("timestamp"), is(ZonedDateTime.parse("2018-01-01T15:30:45.456+01:00[Europe/Paris]")));
    }
}
