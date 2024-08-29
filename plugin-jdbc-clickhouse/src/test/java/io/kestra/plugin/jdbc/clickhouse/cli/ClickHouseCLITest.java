package io.kestra.plugin.jdbc.clickhouse.cli;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.is;

@KestraTest
public class ClickHouseCLITest {

	@Inject
	private RunContextFactory runContextFactory;

	@Test
	void run() throws Exception {
		ClickHouseCLI clickHouseCLI = ClickHouseCLI.builder()
			.id(IdUtils.create())
			.type(ClickHouseCLI.class.getName())
            .commands(List.of("SELECT * FROM system.tables"))
            .build();

		RunContext runContext = TestsUtils.mockRunContext(runContextFactory, clickHouseCLI, Map.of());

		ScriptOutput output = clickHouseCLI.run(runContext);

        assertThat(output.getExitCode(), is(0));
	}

}
