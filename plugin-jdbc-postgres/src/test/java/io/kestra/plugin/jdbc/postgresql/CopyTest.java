package io.kestra.plugin.jdbc.postgresql;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
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
            .url(Property.of(TestUtils.url()))
            .username(Property.of(TestUtils.username()))
            .password(Property.of(TestUtils.password()))
            .ssl(Property.of(TestUtils.ssl()))
            .sslMode(Property.of(TestUtils.sslMode()))
            .sslRootCert(Property.of(TestUtils.ca()))
            .sslCert(Property.of(TestUtils.cert()))
            .sslKey(Property.of(TestUtils.key()))
            .sslKeyPassword(Property.of(TestUtils.keyPass()))
            .format(Property.of(AbstractCopy.Format.CSV))
            .header(Property.of(true))
            .delimiter(Property.of('\t'))
            .escape(Property.of('"'))
            .forceQuote(Property.of(Collections.singletonList("int")))
            .sql(Property.of("SELECT 1 AS int, 't'::bool AS bool UNION SELECT 2 AS int, 'f'::bool AS bool "))
            .build();

        CopyOut.Output runOut = copyOut.run(runContext);
        assertThat(runOut.getRowCount(), is(2L));

        String destination = "d" + IdUtils.create();

        Query create = Query.builder()
            .url(Property.of(TestUtils.url()))
            .username(Property.of(TestUtils.username()))
            .password(Property.of(TestUtils.password()))
            .ssl(Property.of(TestUtils.ssl()))
            .sslMode(Property.of(TestUtils.sslMode()))
            .sslRootCert(Property.of(TestUtils.ca()))
            .sslCert(Property.of(TestUtils.cert()))
            .sslKey(Property.of(TestUtils.key()))
            .sslKeyPassword(Property.of(TestUtils.keyPass()))
            .fetchType(Property.of(FETCH_ONE))
            .sql(Property.of("CREATE TABLE " + destination + " (\n" +
                "    int INT,\n" +
                "    bool BOOL" +
                ");"
            ))
            .build();
        create.run(runContext);

        CopyIn copyIn = CopyIn.builder()
            .url(Property.of(TestUtils.url()))
            .username(Property.of(TestUtils.username()))
            .password(Property.of(TestUtils.password()))
            .ssl(Property.of(TestUtils.ssl()))
            .sslMode(Property.of(TestUtils.sslMode()))
            .sslRootCert(Property.of(TestUtils.ca()))
            .sslCert(Property.of(TestUtils.cert()))
            .sslKey(Property.of(TestUtils.keyNoPass()))
            .from(Property.of(runOut.getUri().toString()))
            .format(Property.of(AbstractCopy.Format.CSV))
            .header(Property.of(true))
            .delimiter(Property.of('\t'))
            .escape(Property.of('"'))
            .table(Property.of(destination))
            .build();

        CopyIn.Output runIn = copyIn.run(runContext);

        assertThat(runIn.getRowCount(), is(2L));
    }
}
