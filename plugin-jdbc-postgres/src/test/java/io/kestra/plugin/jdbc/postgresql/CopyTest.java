package io.kestra.plugin.jdbc.postgresql;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import jakarta.inject.Inject;

import static io.kestra.core.models.tasks.common.FetchType.FETCH_ONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class CopyTest {
    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    void copy() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        CopyOut copyOut = CopyOut.builder()
            .url(TestUtils.url())
            .username(TestUtils.username())
            .password(TestUtils.password())
            .ssl(TestUtils.ssl())
            .sslMode(TestUtils.sslMode())
            .sslRootCert(TestUtils.ca())
            .sslCert(TestUtils.cert())
            .sslKey(TestUtils.key())
            .sslKeyPassword(TestUtils.keyPass())
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
            .url(TestUtils.url())
            .username(TestUtils.username())
            .password(TestUtils.password())
            .ssl(TestUtils.ssl())
            .sslMode(TestUtils.sslMode())
            .sslRootCert(TestUtils.ca())
            .sslCert(TestUtils.cert())
            .sslKey(TestUtils.key())
            .sslKeyPassword(TestUtils.keyPass())
            .fetchType(FETCH_ONE)
            .sql("CREATE TABLE " + destination + " (\n" +
                "    int INT,\n" +
                "    bool BOOL" +
                ");"
            )
            .build();
        create.run(runContext);

        CopyIn copyIn = CopyIn.builder()
            .url(TestUtils.url())
            .username(TestUtils.username())
            .password(TestUtils.password())
            .ssl(TestUtils.ssl())
            .sslMode(TestUtils.sslMode())
            .sslRootCert(TestUtils.ca())
            .sslCert(TestUtils.cert())
            .sslKey(TestUtils.keyNoPass())
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
