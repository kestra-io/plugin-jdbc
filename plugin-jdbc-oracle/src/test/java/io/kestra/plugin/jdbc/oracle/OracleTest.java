package io.kestra.plugin.jdbc.oracle;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * See :
 * - https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT1843
 */
@MicronautTest
public class OracleTest extends AbstractRdbmsTest {
    @Test
    void select() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchOne(true)
            .timeZoneId("Europe/Paris")
            .sql("select * from oracle_types")
            .build();

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        assertThat(runOutput.getRow().get("T_NULL"), is(nullValue()));
        assertThat(runOutput.getRow().get("T_CHAR"), is("aa"));
        assertThat(runOutput.getRow().get("T_VARCHAR"), is("bb"));
        assertThat(runOutput.getRow().get("T_VARCHAR2"), is("cc"));
        assertThat(runOutput.getRow().get("T_NVARCHAR"), is("dd"));
        assertThat(runOutput.getRow().get("T_NVARCHAR2"), is("ee"));
        assertThat(runOutput.getRow().get("T_BLOB"), is("ff".getBytes(StandardCharsets.UTF_8)));
        assertThat(runOutput.getRow().get("T_CLOB"), is("gg"));
        assertThat(runOutput.getRow().get("T_NCLOB"), is("hh"));
        // assertThat(runOutput.getRow().get("T_BFILE"), is(Map.of("name", "STUFF", "bytes", "WD.pdf".getBytes(StandardCharsets.UTF_8))));
        assertThat(runOutput.getRow().get("T_NUMBER"), is(BigDecimal.valueOf(7456123.89)));
        assertThat(runOutput.getRow().get("T_NUMBER_1"), is(BigDecimal.valueOf(7456123.9)));
        assertThat(runOutput.getRow().get("T_NUMBER_2"), is(BigDecimal.valueOf(7456124)));
        assertThat(runOutput.getRow().get("T_NUMBER_3") , is(BigDecimal.valueOf(7456123.89)));
        assertThat(runOutput.getRow().get("T_NUMBER_4") ,is(BigDecimal.valueOf(7456123.9)));
        assertThat(runOutput.getRow().get("T_NUMBER_5"), is(BigDecimal.valueOf(7456100)));
        assertThat(runOutput.getRow().get("T_BINARY_FLOAT"), is(7456123.89F));
        assertThat(runOutput.getRow().get("T_BINARY_DOUBLE"), is(7456123.89D));
        assertThat(runOutput.getRow().get("T_DATE"), is(LocalDate.parse("1992-11-13")));
        assertThat(runOutput.getRow().get("T_TIMESTAMP"), is(LocalDateTime.parse("1998-01-23T06:00:00")));
        assertThat(runOutput.getRow().get("T_TIMESTAMP_TIME_ZONE"), is(ZonedDateTime.parse("1998-01-23T06:00:00-05:00")));
        assertThat(runOutput.getRow().get("T_TIMESTAMP_LOCAL"), is(LocalDateTime.parse("1998-01-23T12:00:00")));
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
            .sql("UPDATE oracle_types SET T_VARCHAR = 'D' WHERE T_VARCHAR = 'bb'")
            .build();

        taskUpdate.run(runContext);


        Query taskGet = Query.builder()
            .url(getUrl())
            .username(getUsername())
            .password(getPassword())
            .fetchOne(true)
            .timeZoneId("Europe/Paris")
            .sql("select T_VARCHAR from oracle_types")
            .build();

        AbstractJdbcQuery.Output runOutput = taskGet.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());
        assertThat(runOutput.getRow().get("T_VARCHAR"), is("D"));
    }

    @Override
    protected String getUrl() {
        return "jdbc:oracle:thin:@localhost:49161:XE";
    }

    @Override
    protected String getUsername() {
        return "system";
    }

    @Override
    protected String getPassword() {
        return "oracle";
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        try {
            RunScript.execute(getConnection(), new StringReader("DROP TABLE oracle_types;"));
        } catch (Exception ignored) {

        }

        executeSqlScript("scripts/oracle.sql");
    }
}
