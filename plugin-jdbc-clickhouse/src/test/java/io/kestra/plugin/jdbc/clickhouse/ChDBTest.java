package io.kestra.plugin.jdbc.clickhouse;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@KestraTest
class ChDBTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void defaultFormatStoresIon() throws Exception {
        assumeDocker();

        ChDB task = ChDB.builder()
            .id(IdUtils.create())
            .type(ChDB.class.getName())
            .query(Property.ofValue("SELECT number AS n FROM system.numbers LIMIT 3"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        runContextFactory.initializer().forExecutor((DefaultRunContext) runContext);

        ChDB.Output output = task.run(runContext);

        assertThat(output.getUri(), is(notNullValue()));
        assertThat(output.getFormat(), is("Ion"));
        assertThat(output.getRowCount(), is(3L));

        List<Map<String, Object>> rows = readIon(runContext, output);
        assertThat(rows, hasSize(3));
        assertThat(((Number) rows.getFirst().get("n")).intValue(), is(0));
    }

    @Test
    void prettyCompactStoresIon() throws Exception {
        assumeDocker();

        ChDB task = ChDB.builder()
            .id(IdUtils.create())
            .type(ChDB.class.getName())
            .query(Property.ofValue("SELECT 1 AS one, 'clickhouse' AS engine"))
            .outputFormat(Property.ofValue("PrettyCompact"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        runContextFactory.initializer().forExecutor((DefaultRunContext) runContext);

        ChDB.Output output = task.run(runContext);

        assertThat(output.getFormat(), is("Ion"));
        assertThat(output.getRowCount(), is(1L));

        List<Map<String, Object>> rows = readIon(runContext, output);
        assertThat(rows, hasSize(1));
        assertThat(((Number) rows.getFirst().get("one")).intValue(), is(1));
        assertThat(rows.getFirst().get("engine"), is("clickhouse"));
    }

    @Test
    void csvWithNamesStoresCsvFile() throws Exception {
        assumeDocker();

        ChDB task = ChDB.builder()
            .id(IdUtils.create())
            .type(ChDB.class.getName())
            .query(Property.ofValue("SELECT number AS n, number * 2 AS doubled FROM system.numbers LIMIT 2"))
            .outputFormat(Property.ofValue("CSVWithNames"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        runContextFactory.initializer().forExecutor((DefaultRunContext) runContext);

        ChDB.Output output = task.run(runContext);

        assertThat(output.getFormat(), is("CSVWithNames"));
        assertThat(output.getRowCount(), is(nullValue()));
        assertThat(output.getUri(), is(notNullValue()));

        String body;
        try (var in = runContext.storage().getFile(output.getUri())) {
            body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }

        assertThat(body, containsString("n"));
        assertThat(body, containsString("doubled"));
        assertThat(body, containsString("0"));
        assertThat(body, containsString("2"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT 1 FORMAT CSV",
        "SELECT 1 format PrettyCompact",
        "SELECT 1\nFORMAT JSONEachRow"
    })
    void rejectsFormatClause(String query) throws Exception {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> runQuery(query)
        );
        assertThat(exception.getMessage(), containsString("FORMAT"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT 1 INTO OUTFILE 'out.csv'",
        "SELECT 1 into outfile 'out.csv'",
        "SELECT 1\nINTO   OUTFILE 'x'"
    })
    void rejectsIntoOutfile(String query) throws Exception {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> runQuery(query)
        );
        assertThat(exception.getMessage(), containsString("INTO OUTFILE"));
    }

    @Test
    void allowsFormatFunctionWithoutFormatClause() throws Exception {
        assumeDocker();

        ChDB.Output output = runQuery("SELECT format('n={}', 1) AS label");

        assertThat(output.getFormat(), is("Ion"));
        assertThat(output.getRowCount(), is(1L));
    }

    private ChDB.Output runQuery(String query) throws Exception {
        ChDB task = ChDB.builder()
            .id(IdUtils.create())
            .type(ChDB.class.getName())
            .query(Property.ofValue(query))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        runContextFactory.initializer().forExecutor((DefaultRunContext) runContext);
        return task.run(runContext);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> readIon(RunContext runContext, ChDB.Output output) throws Exception {
        List<Map<String, Object>> rows = new ArrayList<>();
        try (
            var in = runContext.storage().getFile(output.getUri());
            var reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8), FileSerde.BUFFER_SIZE)
        ) {
            FileSerde.readAll(reader, Map.class).doOnNext(row -> rows.add((Map<String, Object>) row)).blockLast();
        }
        return rows;
    }

    private static void assumeDocker() {
        assumeTrue(isDockerAvailable(), "Docker is required for ChDB integration tests (clickhouse-local container)");
    }

    private static boolean isDockerAvailable() {
        try {
            Process process = new ProcessBuilder("docker", "info")
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .redirectError(ProcessBuilder.Redirect.DISCARD)
                .start();
            boolean finished = process.waitFor(15, java.util.concurrent.TimeUnit.SECONDS);
            return finished && process.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}