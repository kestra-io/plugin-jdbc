package io.kestra.plugin.jdbc.postgresql;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.FlowInputOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static io.kestra.core.models.tasks.common.FetchType.FETCH_ONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@KestraTest(startRunner = true)
@EnabledIfEnvironmentVariable(named = "CI", matches = "true")
public class PgsqlSslTest extends AbstractRdbmsTest {
    @Inject
    private FlowInputOutput flowIO;

    private static final Map<String, Object> INPUTS = ImmutableMap.of(
        "sslRootCert", TestUtils.ca(),
        "sslCert", TestUtils.cert(),
        "sslKey", TestUtils.keyNoPass()
    );

    @Test
    void updateFromFlow() throws Exception {
        Execution execution = runnerUtils.runOne(
            TenantService.MAIN_TENANT,
            "io.kestra.jdbc.postgres",
            "update_postgres",
            null,
            (flow, exec) -> flowIO.readExecutionInputs(flow, exec, INPUTS),
            Duration.ofMinutes(1)
        );

        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
        assertThat(execution.getTaskRunList(), hasSize(2));
    }

    @Test
    void queryWithEncryptedKey() throws Exception {
        Query task = Query.builder()
            .url(Property.ofValue(TestUtils.sslUrl()))
            .username(Property.ofValue(TestUtils.username()))
            .password(Property.ofValue(TestUtils.password()))
            .ssl(Property.ofValue(TestUtils.ssl()))
            .sslMode(Property.ofValue(TestUtils.sslMode()))
            .sslRootCert(Property.ofValue(TestUtils.ca()))
            .sslCert(Property.ofValue(TestUtils.cert()))
            .sslKey(Property.ofValue(TestUtils.key()))
            .sslKeyPassword(Property.ofValue(TestUtils.keyPass()))
            .fetchType(Property.ofValue(FETCH_ONE))
            // pg_hba.conf also accepts plain connections, so assert the session is really encrypted
            .sql(Property.ofValue("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()"))
            .build();

        AbstractJdbcQuery.Output output = task.run(runContextFactory.of(ImmutableMap.of()));

        assertThat(output.getRow().get("ssl"), is(true));
    }

    @Test
    void copyOverSsl() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        CopyOut copyOut = CopyOut.builder()
            .url(Property.ofValue(TestUtils.sslUrl()))
            .username(Property.ofValue(TestUtils.username()))
package io.kestra.plugin.jdbc.postgresql;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.FlowInputOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static io.kestra.core.models.tasks.common.FetchType.FETCH_ONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@KestraTest(startRunner = true)
@EnabledIfEnvironmentVariable(named = "CI", matches = "true")
public class PgsqlSslTest extends AbstractRdbmsTest {
    @Inject
    private FlowInputOutput flowIO;

    private static final Map<String, Object> INPUTS = ImmutableMap.of(
        "sslRootCert", TestUtils.ca(),
        "sslCert", TestUtils.cert(),
        "sslKey", TestUtils.keyNoPass()
    );

    @Test
    void updateFromFlow() throws Exception {
        Execution execution = runnerUtils.runOne(
            TenantService.MAIN_TENANT,
            "io.kestra.jdbc.postgres",
            "update_postgres",
            null,
            (flow, exec) -> flowIO.readExecutionInputs(flow, exec, INPUTS),
            Duration.ofMinutes(1)
        );

        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
        assertThat(execution.getTaskRunList(), hasSize(2));
    }

    @Test
    void queryWithEncryptedKey() throws Exception {
        Query task = Query.builder()
            .url(Property.ofValue(TestUtils.sslUrl()))
            .username(Property.ofValue(TestUtils.username()))
            .password(Property.ofValue(TestUtils.password()))
            .ssl(Property.ofValue(TestUtils.ssl()))
            .sslMode(Property.ofValue(TestUtils.sslMode()))
            .sslRootCert(Property.ofValue(TestUtils.ca()))
            .sslCert(Property.ofValue(TestUtils.cert()))
            .sslKey(Property.ofValue(TestUtils.key()))
            .sslKeyPassword(Property.ofValue(TestUtils.keyPass()))
            .fetchType(Property.ofValue(FETCH_ONE))
            // pg_hba.conf also accepts plain connections, so assert the session is really encrypted
            .sql(Property.ofValue("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()"))
            .build();

        AbstractJdbcQuery.Output output = task.run(runContextFactory.of(ImmutableMap.of()));

        assertThat(output.getRow().get("ssl"), is(true));
    }

    @Test
    void copyOverSsl() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        CopyOut copyOut = CopyOut.builder()
            .url(Property.ofValue(TestUtils.sslUrl()))
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
            .sql(Property.ofValue("SELECT 1 AS int, 't'::bool AS bool UNION SELECT 2 AS int, 'f'::bool AS bool"))
            .build();

        CopyOut.Output runOut = copyOut.run(runContext);
        assertThat(runOut.getRowCount(), is(2L));

        String destination = "d" + IdUtils.create();
        Query.builder()
            .url(Property.ofValue(TestUtils.sslUrl()))
            .username(Property.ofValue(TestUtils.username()))
            .password(Property.ofValue(TestUtils.password()))
            .sslMode(Property.ofValue(TestUtils.sslMode()))
            .sslRootCert(Property.ofValue(TestUtils.ca()))
            .sslCert(Property.ofValue(TestUtils.cert()))
            .sslKey(Property.ofValue(TestUtils.keyNoPass()))
            .fetchType(Property.ofValue(FETCH_ONE))
            .sql(Property.ofValue("CREATE TABLE " + destination + " (int INT, bool BOOL);"))
            .build()
            .run(runContext);

        CopyIn copyIn = CopyIn.builder()
            .url(Property.ofValue(TestUtils.sslUrl()))
            .username(Property.ofValue(TestUtils.username()))
            .password(Property.ofValue(TestUtils.password()))
            .sslMode(Property.ofValue(TestUtils.sslMode()))
            .sslRootCert(Property.ofValue(TestUtils.ca()))
            .sslCert(Property.ofValue(TestUtils.cert()))
            .sslKey(Property.ofValue(TestUtils.keyNoPass()))
            .from(Property.ofValue(runOut.getUri().toString()))
            .format(Property.ofValue(AbstractCopy.Format.CSV))
            .header(Property.ofValue(true))
            .table(Property.ofValue(destination))
            .build();

        assertThat(copyIn.run(runContext).getRowCount(), is(2L));
    }

    @Override
    protected String getUrl() {
        return TestUtils.sslUrl();
    }

    @Override
    protected String getUsername() {
        return TestUtils.username();
    }

    @Override
    protected String getPassword() {
        return TestUtils.password();
    }

    @Override
    protected Connection getConnection() throws SQLException {
        Properties props = new Properties();
        props.put("user", getUsername());
        props.put("password", getPassword());

        return DriverManager.getConnection(getUrl(), props);
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/postgres.sql");
    }
}