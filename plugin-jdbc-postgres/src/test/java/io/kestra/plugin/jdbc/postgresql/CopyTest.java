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
            .url(Property.ofValue(TestUtils.url()))
            .username(Property.ofValue(TestUtils.username()))
            .password(Property.ofValue(TestUtils.password()))
            .ssl(Property.ofValue(TestUtils.ssl()))
            .sslMode(Property.ofValue(TestUtils.sslMode()))
            .sslRootCert(Property.ofValue(TestUtils.ca()))
            .sslCert(Property.ofValue(TestUtils.cert()))
            .sslKey(Property.ofValue(TestUtils.key()))
            .sslKeyPassword(Property.ofValue(TestUtils.keyPass()))
            .format(Property.ofValue(AbstractCopy.Format.CSV))
            .header(Property.ofValue(true))
            .delimiter(Property.ofValue('\t'))
            .escape(Property.ofValue('"'))
            .forceQuote(Property.ofValue(Collections.singletonList("int")))
            .sql(Property.ofValue("SELECT 1 AS int, 't'::bool AS bool UNION SELECT 2 AS int, 'f'::bool AS bool "))
            .build();

        CopyOut.Output runOut = copyOut.run(runContext);
        assertThat(runOut.getRowCount(), is(2L));

        String destination = "d" + IdUtils.create();

        Query create = Query.builder()
            .url(Property.ofValue(TestUtils.url()))
            .username(Property.ofValue(TestUtils.username()))
            .password(Property.ofValue(TestUtils.password()))
            .ssl(Property.ofValue(TestUtils.ssl()))
            .sslMode(Property.ofValue(TestUtils.sslMode()))
            .sslRootCert(Property.ofValue(TestUtils.ca()))
            .sslCert(Property.ofValue(TestUtils.cert()))
            .sslKey(Property.ofValue(TestUtils.key()))
            .sslKeyPassword(Property.ofValue(TestUtils.keyPass()))
            .fetchType(Property.ofValue(FETCH_ONE))
            .sql(Property.ofValue("CREATE TABLE " + destination + " (\n" +
                "    int INT,\n" +
                "    bool BOOL" +
                ");"
            ))
            .build();
        create.run(runContext);

        CopyIn copyIn = CopyIn.builder()
            .url(Property.ofValue(TestUtils.url()))
            .username(Property.ofValue(TestUtils.username()))
            .password(Property.ofValue(TestUtils.password()))
            .ssl(Property.ofValue(TestUtils.ssl()))
            .sslMode(Property.ofValue(TestUtils.sslMode()))
            .sslRootCert(Property.ofValue(TestUtils.ca()))
            .sslCert(Property.ofValue(TestUtils.cert()))
            .sslKey(Property.ofValue(TestUtils.keyNoPass()))
            .from(Property.ofValue(runOut.getUri().toString()))
            .format(Property.ofValue(AbstractCopy.Format.CSV))
            .header(Property.ofValue(true))
            .delimiter(Property.ofValue('\t'))
            .escape(Property.ofValue('"'))
            .table(Property.ofValue(destination))
            .build();

        CopyIn.Output runIn = copyIn.run(runContext);

        assertThat(runIn.getRowCount(), is(2L));
    }
}
