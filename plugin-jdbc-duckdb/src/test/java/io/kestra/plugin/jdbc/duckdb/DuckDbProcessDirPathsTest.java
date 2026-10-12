package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.LoggerFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.kestra.core.models.tasks.common.FetchType.FETCH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@Execution(ExecutionMode.SAME_THREAD)
class DuckDbProcessDirPathsTest {
    private static final Path PROCESS_DIR = Path.of(System.getProperty("user.dir"));

    @Inject
    private RunContextFactory runContextFactory;

    private final List<ILoggingEvent> logs = new CopyOnWriteArrayList<>();
    private ListAppender<ILoggingEvent> appender;

    @BeforeEach
    void attachAppender() {
        logs.clear();
        appender = new ListAppender<>() {
            @Override
            protected void append(ILoggingEvent event) {
                logs.add(event);
            }
        };
        appender.start();
        ((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        ((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).detachAppender(appender);
    }

    private static String csv() {
        return "id,name\n1,alpha\n";
    }

    private List<ILoggingEvent> warnings() {
        return logs.stream().filter(l -> l.getLevel() == Level.WARN).toList();
    }

    private boolean hasWarningFor(String fileName) {
        return warnings().stream().anyMatch(l -> l.getFormattedMessage().contains("'" + fileName + "'")
            && l.getFormattedMessage().contains("may have been written by this task")
            && l.getFormattedMessage().contains("'{{ workingDir }}/" + fileName + "'")
            && l.getFormattedMessage().contains("'{{ outputFiles.<key> }}'")
            && !l.getFormattedMessage().contains("{{{{"));
    }

    @Test
    void relativeReadThroughInputFiles() throws Exception {
        var key = IdUtils.create() + ".csv";
        var query = Query.builder()
            .id(IdUtils.create()).type(Query.class.getName())
            .fetchType(Property.ofValue(FETCH))
            .inputFiles(Map.of(key, csv()))
            .sql(Property.ofValue("SELECT id, name FROM read_csv_auto('" + key + "')"))
            .build();

        var output = query.run(runContextFactory.of(Map.of()));

        assertThat(output.getRows(), hasSize(1));
        assertThat(output.getRows().getFirst().get("name"), is("alpha"));
    }

    @Test
    void failsWhenStrayFileShadowsRelativeInput() throws Exception {
        var key = IdUtils.create() + ".csv";
        var stray = PROCESS_DIR.resolve(key);
        try {
            Files.writeString(stray, "id,name\n9,stale\n");
            var query = Query.builder()
                .id(IdUtils.create()).type(Query.class.getName())
                .fetchType(Property.ofValue(FETCH))
                .inputFiles(Map.of(key, csv()))
                .sql(Property.ofValue("SELECT * FROM read_csv_auto('" + key + "')"))
                .build();

            var e = assertThrows(IllegalStateException.class, () -> query.run(runContextFactory.of(Map.of())));
            assertThat(e.getMessage(), containsString(stray.toAbsolutePath().toString()));
            assertThat(e.getMessage(), containsString("{{ workingDir }}/" + key));
        } finally {
            Files.deleteIfExists(stray);
        }
    }

    @Test
    void queriesFailsWhenStrayFileShadowsRelativeInput() throws Exception {
        var key = IdUtils.create() + ".csv";
        var stray = PROCESS_DIR.resolve(key);
        try {
            Files.writeString(stray, "id,name\n9,stale\n");
            var queries = Queries.builder()
                .id(IdUtils.create()).type(Queries.class.getName())
                .fetchType(Property.ofValue(FETCH))
                .inputFiles(Map.of(key, csv()))
                .sql(Property.ofValue("SELECT * FROM read_csv_auto('" + key + "');"))
                .build();

            var e = assertThrows(IllegalStateException.class, () -> queries.run(runContextFactory.of(Map.of())));
            assertThat(e.getMessage(), containsString(stray.toAbsolutePath().toString()));
        } finally {
            Files.deleteIfExists(stray);
        }
    }

    @Test
    void noFalsePositiveWhenWorkingDirIsUsed() throws Exception {
        var key = IdUtils.create() + ".csv";
        var stray = PROCESS_DIR.resolve(key);
        try {
            Files.writeString(stray, "id,name\n9,stale\n");
            var query = Query.builder()
                .id(IdUtils.create()).type(Query.class.getName())
                .fetchType(Property.ofValue(FETCH))
                .inputFiles(Map.of(key, csv()))
                .sql(Property.ofExpression("SELECT id, name FROM read_csv_auto('{{ workingDir }}/" + key + "')"))
                .build();

            var output = query.run(runContextFactory.of(Map.of()));

            assertThat(output.getRows(), hasSize(1));
            assertThat(output.getRows().getFirst().get("name"), is("alpha"));
        } finally {
            Files.deleteIfExists(stray);
        }
    }

    @Test
    void warnsOnRelativeWrite() throws Exception {
        var name = IdUtils.create() + ".csv";
        var written = PROCESS_DIR.resolve(name);
        try {
            var query = Query.builder()
                .id(IdUtils.create()).type(Query.class.getName())
                .sql(Property.ofValue("COPY (SELECT 1 AS id) TO '" + name + "' (HEADER, DELIMITER ',')"))
                .build();

            query.run(runContextFactory.of(Map.of()));

            assertThat(Files.exists(written), is(true));
            assertThat(hasWarningFor(name), is(true));
        } finally {
            Files.deleteIfExists(written);
        }
    }

    @Test
    void queriesWarnsOnRelativeWrite() throws Exception {
        var name = IdUtils.create() + ".csv";
        var written = PROCESS_DIR.resolve(name);
        try {
            var queries = Queries.builder()
                .id(IdUtils.create()).type(Queries.class.getName())
                .sql(Property.ofValue("COPY (SELECT 1 AS id) TO '" + name + "' (HEADER, DELIMITER ',');"))
                .build();

            queries.run(runContextFactory.of(Map.of()));

            assertThat(hasWarningFor(name), is(true));
        } finally {
            Files.deleteIfExists(written);
        }
    }

    @Test
    void noWarningForWorkingDirOrOutputFilesWrites() throws Exception {
        var name = IdUtils.create();

        var query = Query.builder()
            .id(IdUtils.create()).type(Query.class.getName())
            .outputFiles(Property.ofValue(List.of("out")))
            .sql(Property.ofExpression("COPY (SELECT 1 AS id) TO '{{ workingDir }}/" + name + ".csv' (HEADER, DELIMITER ',')"))
            .build();
        query.run(runContextFactory.of(Map.of()));

        var second = Query.builder()
            .id(IdUtils.create()).type(Query.class.getName())
            .outputFiles(Property.ofValue(List.of("out")))
            .sql(Property.ofExpression("COPY (SELECT 2 AS id) TO '{{ outputFiles.out }}' (HEADER, DELIMITER ',')"))
            .build();
        second.run(runContextFactory.of(Map.of()));

        assertThat(warnings().stream().filter(l -> l.getFormattedMessage().contains("process directory")).toList(), is(empty()));
    }
}
