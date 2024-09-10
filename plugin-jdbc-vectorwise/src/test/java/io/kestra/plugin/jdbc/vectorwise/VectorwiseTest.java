package io.kestra.plugin.jdbc.vectorwise;

import com.google.common.collect.ImmutableMap;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.*;

import static io.kestra.core.models.tasks.common.FetchType.FETCH_ONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


@KestraTest
@Disabled("no server for unit test")
public class VectorwiseTest extends AbstractRdbmsTest {
    @Value("jdbc:ingres://url:port/db")
    protected String url;

    @Value("user")
    protected String user;

    @Value("password")
    protected String password;

    @Test
    void select() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        Query task = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchType(FETCH_ONE)
            .timeZoneId("Europe/Paris")
            .sql(new String (Files.readAllBytes( Paths.get("src/test/resources/scripts/vectorwise.sql"))))
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get("tinyint"), is(125));
        assertThat(runOutput.getRow().get("smallint"), is(32000));
        assertThat(runOutput.getRow().get("integer"), is(2147483640));
        assertThat(runOutput.getRow().get("bigint"), is(9007199254740991L));
        assertThat(runOutput.getRow().get("decimal"), is(new BigDecimal("1.55")));
        assertThat(runOutput.getRow().get("float8"), is(1.12365125789541));
        assertThat(runOutput.getRow().get("float"), is(1.2F));
        assertThat(runOutput.getRow().get("money"), is(new BigDecimal("100.50")));
        assertThat(runOutput.getRow().get("char"), is("y"));
        assertThat(runOutput.getRow().get("varchar"), is("yoplait"));
        assertThat(runOutput.getRow().get("nchar"), is("tes"));
        assertThat(runOutput.getRow().get("nvarchar"), is("autret"));
        assertThat(runOutput.getRow().get("datetime"), is(LocalDateTime.parse("1900-10-04T22:23:00.000")));
        assertThat(runOutput.getRow().get("ansidate"), is(LocalDate.parse("2006-05-16")));
        assertThat(runOutput.getRow().get("ansidatetime"), is(LocalDate.parse("2006-05-16")));
        assertThat(runOutput.getRow().get("timenotz"), is(LocalTime.parse("04:05:30")));
        assertThat(runOutput.getRow().get("timetz"), is(LocalTime.parse("05:05:30")));
        assertThat(runOutput.getRow().get("timelocaltz"), is(LocalTime.parse("05:05:30")));
        assertThat(runOutput.getRow().get("timestampnotz"), is(LocalDateTime.parse("2006-05-16T00:00:00.000")));
        assertThat(runOutput.getRow().get("timestamptz"), is(ZonedDateTime.parse("2006-05-16T02:00:00.000+02:00[Europe/Paris]")));
        assertThat(runOutput.getRow().get("timestamplocaltz"), is(ZonedDateTime.parse("2006-05-16T02:00:00.000+02:00[Europe/Paris]")));
        assertThat(runOutput.getRow().get("intervalytom"), is("2007-10"));
        assertThat(runOutput.getRow().get("intervaldtos"), is("4 00:00:00.000000"));
        assertThat(runOutput.getRow().get("boolean"), is(true));

    }

    @Override
    protected String getUrl() {
        return this.url;
    }

    @Override
    protected String getUsername() {
        return this.user;
    }

    @Override
    protected String getPassword() {
        return this.password;
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
         executeSqlScript("scripts/vectorwise.sql");
    }
}
