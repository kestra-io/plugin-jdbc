package io.kestra.plugin.jdbc.snowflake;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
@Disabled("Create a Snowflake account for unit testing")
public class SnowflakeCLITest {

    @Inject
    private RunContextFactory runContextFactory;

    protected String account = "";
    protected String username = "";
    protected String password = "";

    @Test
    void run() throws Exception {

        var snowflakeCLI = SnowflakeCLI.builder()
            .id(IdUtils.create())
            .type(SnowflakeCLI.class.getName())
            .account(Property.of(account))
            .username(Property.of(username))
            .password(Property.of(password))
            .commands(
                Property.of(List.of("snow connection test")))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, snowflakeCLI, Map.of());

        ScriptOutput output = snowflakeCLI.run(runContext);

        assertThat(output.getExitCode(), is(0));
    }

}
