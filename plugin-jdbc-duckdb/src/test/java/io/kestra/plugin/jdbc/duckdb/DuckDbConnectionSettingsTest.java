package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;

import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@KestraTest
class DuckDbConnectionSettingsTest {
    @Inject
    RunContextFactory runContextFactory;

    @Inject
    StorageInterface storageInterface;

    @ParameterizedTest
    @CsvSource({
        "v1.4.5, v1.4.0+",
        "v1.5.6, v1.5.0+",
        "v2.0.0, v2.0.0+",
        "latest, v2.0.0+"
    })
    void queryCreatesAndExportsTheRenderedStorageVersion(String version, String expectedTag) throws Exception {
        var output = Query.builder()
            .storageVersion(Property.ofExpression("{{ vars.target }}"))
            .communityExtensions(Property.ofValue(List.of()))
            .sql(Property.ofValue("CREATE TABLE events AS SELECT 42::INTEGER AS id"))
            .fetchType(Property.ofValue(FetchType.NONE))
            .outputDbFile(Property.ofValue(true))
            .build().run(runContextFactory.of(Map.of("vars", Map.of("target", version))));

        assertStorageTag(output.getDatabaseUri(), expectedTag);
        var reopened = Query.builder()
            .databaseUri(Property.ofValue(output.getDatabaseUri().toString()))
            .communityExtensions(Property.ofValue(List.of()))
            .sql(Property.ofValue("SELECT id FROM events"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(runContextFactory.of(Map.of()));
        assertThat(reopened.getRow(), is(Map.of("id", 42)));
    }

    @Test
    void queriesApplyStorageVersionBeforeCreatingTheDatabase() throws Exception {
        var output = Queries.builder()
            .storageVersion(Property.ofExpression("{{ vars.target }}"))
            .communityExtensions(Property.ofValue(List.of()))
            .sql(Property.ofValue("CREATE TABLE events AS SELECT 42 AS id; SELECT id FROM events;"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .outputDbFile(Property.ofValue(true))
            .build().run(runContextFactory.of(Map.of("vars", Map.of("target", "v1.5.6"))));

        assertThat(output.getOutputs().getFirst().getRow(), is(Map.of("id", 42)));
        assertStorageTag(output.getDatabaseUri(), "v1.5.0+");
    }

    @Test
    void triggerForwardsTheRenderedStorageVersion() throws Exception {
        var output = Trigger.builder()
            .storageVersion(Property.ofExpression("{{ vars.target }}"))
            .communityExtensions(Property.ofValue(List.of()))
            .sql(Property.ofValue("SELECT 42 AS id"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .outputDbFile(Property.ofValue(true))
            .build().runQuery(runContextFactory.of(Map.of("vars", Map.of("target", "v1.5.6"))));

        assertThat(output.getRow(), is(Map.of("id", 42)));
        assertStorageTag(((Query.Output) output).getDatabaseUri(), "v1.5.0+");
    }

    @Test
    void omittedStorageVersionKeepsTheNewDatabaseDefault() throws Exception {
        var output = Query.builder()
            .communityExtensions(Property.ofValue(List.of()))
            .sql(Property.ofValue("CREATE TABLE events AS SELECT 42 AS id"))
            .fetchType(Property.ofValue(FetchType.NONE))
            .outputDbFile(Property.ofValue(true))
            .build().run(runContextFactory.of(Map.of()));

        assertStorageTag(output.getDatabaseUri(), "v2.0.0+");

        var reopened = Query.builder()
            .databaseUri(Property.ofValue(output.getDatabaseUri().toString()))
            .storageVersion(Property.ofValue("v1.5.6"))
            .communityExtensions(Property.ofValue(List.of()))
            .sql(Property.ofValue("INSERT INTO events VALUES (43)"))
            .fetchType(Property.ofValue(FetchType.NONE))
            .outputDbFile(Property.ofValue(true))
            .build().run(runContextFactory.of(Map.of()));
        assertStorageTag(reopened.getDatabaseUri(), "v2.0.0+");
    }

    @Test
    void explicitLatestDoesNotUpgradeAnExistingLegacyFile() throws Exception {
        var source = URI.create("/" + IdUtils.create());
        try (var input = Objects.requireNonNull(getClass().getResourceAsStream("/db/duckdb-1.5.6.db"))) {
            source = storageInterface.put(TenantService.MAIN_TENANT, null, source, input);
        }
        var output = Query.builder()
            .databaseUri(Property.ofValue(source.toString()))
            .storageVersion(Property.ofValue("latest"))
            .communityExtensions(Property.ofValue(List.of()))
            .sql(Property.ofValue("INSERT INTO legacy_events VALUES (3, 'retained format')"))
            .fetchType(Property.ofValue(FetchType.NONE))
            .outputDbFile(Property.ofValue(true))
            .build().run(runContextFactory.of(Map.of()));

        assertStorageTag(output.getDatabaseUri(), "v1.0.0+");
        assertStorageTag(source, "v1.0.0+");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "  "})
    void rejectsBlankStorageVersions(String version) {
        var task = Query.builder()
            .storageVersion(Property.ofExpression("{{ vars.target }}"))
            .communityExtensions(Property.ofValue(List.of()))
            .sql(Property.ofValue("SELECT 42"))
            .build();

        var failure = assertThrows(IllegalArgumentException.class,
            () -> task.run(runContextFactory.of(Map.of("vars", Map.of("target", version)))));
        assertThat(failure.getMessage(), containsString("storageVersion"));
    }

    @Test
    void rejectsUnknownStorageVersions() {
        var task = Query.builder()
            .storageVersion(Property.ofValue("not-a-version"))
            .communityExtensions(Property.ofValue(List.of()))
            .sql(Property.ofValue("SELECT 42"))
            .build();

        var failure = assertThrows(SQLException.class, () -> task.run(runContextFactory.of(Map.of())));
        assertThat(failure.getMessage(), containsString("storage_compatibility_version"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void warnsForRenderedMotherDuckUrlsBeforeConnectingWithoutLoggingTheToken(boolean multiQuery) throws Exception {
        var logger = mock(Logger.class);
        var context = spy(runContextFactory.of(Map.of("vars", Map.of(
            "url", "jdbc:duckdb:md:example?motherduck_token=token-must-stay-private"
        ))));
        doReturn(logger).when(context).logger();
        var url = Property.<String>ofExpression("{{ vars.url }}");
        if (multiQuery) {
            var task = spy(Queries.builder().url(url).sql(Property.ofValue("SELECT 42")).build());
            doThrow(new SQLException("Connection blocked in test")).when(task).connection(any());
            assertThrows(Exception.class, () -> task.run(context));
        } else {
            var task = spy(Query.builder().url(url).sql(Property.ofValue("SELECT 42")).build());
            doThrow(new SQLException("Connection blocked in test")).when(task).connection(any());
            assertThrows(SQLException.class, () -> task.run(context));
        }

        var warning = ArgumentCaptor.forClass(String.class);
        verify(logger).warn(warning.capture());
        assertThat(warning.getValue(), containsString("MotherDuck"));
        assertThat(warning.getValue(), containsString("DuckDB 2.0"));
        assertThat(warning.getValue(), not(containsString("token-must-stay-private")));
        assertThat(warning.getValue(), not(containsString("jdbc:")));
    }

    @Test
    void doesNotWarnAboutMotherDuckForLocalDatabases() throws Exception {
        var logger = mock(Logger.class);
        var context = spy(runContextFactory.of(Map.of()));
        doReturn(logger).when(context).logger();
        Query.builder()
            .communityExtensions(Property.ofValue(List.of()))
            .sql(Property.ofValue("SELECT 42"))
            .build().run(context);

        verify(logger, never()).warn(contains("MotherDuck"));
    }

    private void assertStorageTag(URI uri, String expected) throws Exception {
        var output = Query.builder()
            .databaseUri(Property.ofValue(uri.toString()))
            .communityExtensions(Property.ofValue(List.of()))
            .sql(Property.ofValue("SELECT tags['storage_version'] AS version FROM duckdb_databases() WHERE database_name=current_database()"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build().run(runContextFactory.of(Map.of()));
        assertThat(output.getRow(), is(Map.of("version", expected)));
    }
}
