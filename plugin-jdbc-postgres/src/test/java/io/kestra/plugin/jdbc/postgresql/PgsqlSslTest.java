package io.kestra.plugin.jdbc.postgresql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;
import io.kestra.core.runners.FlowInputOutput;
import io.kestra.core.tenant.TenantService;
import io.kestra.plugin.jdbc.AbstractRdbmsTest;
import jakarta.inject.Inject;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

@KestraTest(startRunner = true)
public class PgsqlSslTest extends AbstractRdbmsTest {
  @Inject private FlowInputOutput flowIO;

  private static final Map<String, Object> INPUTS =
      ImmutableMap.of("sslRootCert", TestUtils.ca(), "sslCert",
                      TestUtils.cert(), "sslKey", TestUtils.keyNoPass());

  @Test
  void updateFromFlow() throws Exception {
    Execution execution = runnerUtils.runOne(
        TenantService.MAIN_TENANT, "io.kestra.jdbc.postgres", "update_postgres",
        null,
        (flow, exec)
            -> flowIO.readExecutionInputs(flow, exec, INPUTS),
        Duration.ofMinutes(1));

    assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    assertThat(execution.getTaskRunList(), hasSize(2));
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
  protected void initDatabase()
      throws SQLException, FileNotFoundException, URISyntaxException {
    executeSqlScript("scripts/postgres.sql");
  }
}