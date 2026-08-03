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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@KestraTest
class ChDBTest {

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeEach
    void requireDocker() {
        assumeTrue(isDockerAvailable(), "Docker is required for ChDB integration tests (clickhouse-local container)");
    }

    @Test
    void defaultFormatStoresIon() throws Exception {
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
        assertThat(output.getSize(), is(3L));

        List<Map<String, Object>> rows = readIon(runContext, output);
        assertThat(rows, hasSize(3));
        assertThat(((Number) rows.getFirst().get("n")).intValue(), is(0));
    }

    @Test
    void prettyCompactStoresIon() throws Exception {
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
        assertThat(output.getSize(), is(1L));

        List<Map<String, Object>> rows = readIon(runContext, output);
        assertThat(rows, hasSize(1));
        assertThat(((Number) rows.getFirst().get("one")).intValue(), is(1));
        assertThat(rows.getFirst().get("engine"), is("clickhouse"));
    }

    @Test
    void csvWithNamesStoresCsvFile() throws Exception {
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
        assertThat(output.getSize(), is(nullValue()));
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

    private static boolean isDockerAvailable() {
        try {
            Process process = new ProcessBuilder("docker", "info")
                .redirectErrorStream(true)
                .start();
            boolean finished = process.waitFor(15, java.util.concurrent.TimeUnit.SECONDS);
            return finished && process.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}