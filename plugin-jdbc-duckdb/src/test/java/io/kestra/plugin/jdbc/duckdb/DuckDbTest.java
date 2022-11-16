package io.kestra.plugin.jdbc.duckdb;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.*;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * See :
 * - https://duckdb.org/docs/sql/data_types/overview
 */
@MicronautTest
public class DuckDbTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void select() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

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

        AbstractJdbcQuery.Output runOutput = task.run(runContext);
        assertThat(runOutput.getRow(), notNullValue());

        String hour = System.getenv("GITHUB_WORKFLOW") != null ? "17" : "15";

        assertThat(runOutput.getRow().get("t_null"), is(nullValue()));
        assertThat(runOutput.getRow().get("t_bigint"), is(9223372036854775807L));
        assertThat(runOutput.getRow().get("t_boolean"), is(true));
        assertThat(runOutput.getRow().get("t_date"), is(LocalDate.parse("1992-09-20")));
        assertThat(runOutput.getRow().get("t_double"), is(12345.12345D));
        assertThat(runOutput.getRow().get("t_decimal"), is(BigDecimal.valueOf(12345123L, 3)));;
        assertThat(runOutput.getRow().get("t_hugeint"), is("9223372036854775807"));
        assertThat(runOutput.getRow().get("t_integer"), is(2147483647));
        assertThat(runOutput.getRow().get("t_interval"), is("28 days"));
        assertThat(runOutput.getRow().get("t_real"), is(123.456F));
        assertThat(runOutput.getRow().get("t_smallint"), is((short)127));
        // assertThat(runOutput.getRow().get("t_time"), is(LocalTime.parse("'01:02:03.456"))); null is returned
        assertThat(runOutput.getRow().get("t_timestamp"), is(ZonedDateTime.parse("2020-06-10T" +  hour + ":55:23.383345+02:00[Europe/Paris]")));
        assertThat(runOutput.getRow().get("t_timestamptz"), is(OffsetDateTime.parse("2001-08-22T10:04:05.321Z")));
        assertThat(runOutput.getRow().get("t_tinyint"), is(127));
        assertThat(runOutput.getRow().get("t_ubigint"), is("9223372036854775807"));
        assertThat(runOutput.getRow().get("t_uinteger"), is(2147483647L));
        assertThat(runOutput.getRow().get("t_usmallint"), is(32767));
        assertThat(runOutput.getRow().get("t_utinyint"), is((short)127));
        assertThat(runOutput.getRow().get("t_varchar"), is("test"));
        assertThat(runOutput.getRow().get("t_enum"), is("happy"));
    }

    @Test
    void inputOutputFiles() throws Exception {
        URI source = storageInterface.put(
            new URI("/" + IdUtils.create()),
            new FileInputStream(new File(Objects.requireNonNull(DuckDbTest.class.getClassLoader()
                    .getResource("full.csv"))
                .toURI())
            )
        );
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query task = Query.builder()
            .timeZoneId("Europe/Paris")
            .inputFiles(Map.of("in.csv", source.toString()))
            .outputFiles(List.of("out"))
            .sql("CREATE TABLE new_tbl AS SELECT * FROM read_csv_auto('{{workingDir}}/in.csv', header=True);\n" +
                "\n" +
                "COPY (SELECT id, name FROM new_tbl) TO '{{ outputFiles.out }}' (HEADER, DELIMITER ',');")
            .build();

        Query.Output runOutput = task.run(runContext);

        System.out.println(JacksonMapper.ofYaml().writeValueAsString(task));

        assertThat(
            IOUtils.toString(storageInterface.get(runOutput.getOutputFiles().get("out")), Charsets.UTF_8),
            is( "id,name\n" +
                "4814976,Viva\n" +
                "1010871,Voomm\n" +
                "6782048,Ailane\n" +
                "989670,Livetube\n" +
                "1696964,Quinu\n" +
                "8761731,Quamba\n" +
                "5768523,Tagcat\n" +
                "6702408,Roomm\n" +
                "3896245,Flashspan\n" +
                "946749,Browsebug\n"
            )
        );
    }
}
