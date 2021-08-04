package io.kestra.plugin.jdbc.postgresql;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.time.*;
import java.util.Collections;
import java.util.Map;

import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
public class CopyTest {
    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    void copy() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        CopyOut copyOut = CopyOut.builder()
            .url("jdbc:postgresql://127.0.0.1:56982/")
            .username("postgres")
            .password("pg_passwd")
            .format(AbstractCopy.Format.CSV)
            .header(true)
            .delimiter('\t')
            .escape('"')
            .forceQuote(Collections.singletonList("int"))
            .sql("SELECT 1 AS int, 't'::bool AS bool UNION SELECT 2 AS int, 'f'::bool AS bool ")
            .build();

        CopyOut.Output runOut = copyOut.run(runContext);
        assertThat(runOut.getRowCount(), is(2L));

        String destination = "d" + IdUtils.create();

        Query create = Query.builder()
            .url("jdbc:postgresql://127.0.0.1:56982/")
            .username("postgres")
            .password("pg_passwd")
            .fetchOne(true)
            .sql("CREATE TABLE " + destination + " (\n" +
                "    int INT,\n" +
                "    bool BOOL" +
                ");"
            )
            .build();
        create.run(runContext);

        CopyIn copyIn = CopyIn.builder()
            .url("jdbc:postgresql://127.0.0.1:56982/")
            .username("postgres")
            .password("pg_passwd")
            .from(runOut.getUri().toString())
            .format(AbstractCopy.Format.CSV)
            .header(true)
            .delimiter('\t')
            .escape('"')
            .table(destination)
            .build();

        CopyIn.Output runIn = copyIn.run(runContext);

        assertThat(runIn.getRowCount(), is(2L));
    }
}
