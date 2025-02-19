package io.kestra.plugin.jdbc.clickhouse;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
public class ClickHouseLocalCLITest {

	@Inject
	private RunContextFactory runContextFactory;

	@Test
	void run() throws Exception {
		ClickHouseLocalCLI clickhouseLocalCLI = ClickHouseLocalCLI.builder()
			.id(IdUtils.create())
			.type(ClickHouseLocalCLI.class.getName())
            .commands(Property.of(List.of("SELECT * FROM system.tables")))
            .build();

		RunContext runContext = TestsUtils.mockRunContext(runContextFactory, clickhouseLocalCLI, Map.of());

		ScriptOutput output = clickhouseLocalCLI.run(runContext);

        assertThat(output.getExitCode(), is(0));
	}

}
