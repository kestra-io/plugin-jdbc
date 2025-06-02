package io.kestra.plugin.jdbc.clickhouse;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.*;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.runner.docker.Docker;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
	title = "Run clickhouse-local CLI commands."
)
@Plugin(
	examples = {
		@Example(
			title = "Run clickhouse-local commands",
            full = true,
            code = {
                """
                id: clickhouse-local
                namespace: company.team
                tasks:
                  - id: query
                    type: io.kestra.plugin.clickhouse.ClickHouseLocalCLI
                    commands:
                      - SELECT count() FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')
                """
            }
		)
	}
)
public class ClickHouseLocalCLI extends Task implements RunnableTask<ScriptOutput>, NamespaceFilesInterface, InputFilesInterface, OutputFilesInterface {

	public static final String DEFAULT_IMAGE = "clickhouse/clickhouse-server:latest";

	@Schema(
		title = "The commands to run before main list of commands."
	)
	protected Property<List<String>> beforeCommands;

	@Schema(
		title = "The commands to run."
	)
    @NotNull
	protected Property<List<String>> commands;

	@Schema(
		title = "Additional environment variables for the current process."
	)
	@PluginProperty(dynamic = true)
	protected Map<String, String> env;

	@Schema(
        title = "The task runner to use."
	)
	@Valid
	@PluginProperty
	@Builder.Default
	private TaskRunner<?> taskRunner = Docker.instance();

	@Schema(
		title = "The Clickhouse container image."
	)
	@PluginProperty(dynamic =true)
	@Builder.Default
	private String containerImage = DEFAULT_IMAGE;

	private NamespaceFiles namespaceFiles;

	private Object inputFiles;

	private Property<List<String>> outputFiles;

	@Override
	public ScriptOutput run(RunContext runContext) throws Exception {
        var renderedOutputFiles = runContext.render(this.outputFiles).asList(String.class);

		return new CommandsWrapper(runContext)
			.withWarningOnStdErr(true)
			.withTaskRunner(this.taskRunner)
			.withContainerImage(this.containerImage)
			.withEnv(Optional.ofNullable(env).orElse(new HashMap<>()))
			.withNamespaceFiles(namespaceFiles)
			.withInputFiles(inputFiles)
			.withOutputFiles(renderedOutputFiles.isEmpty() ? null : renderedOutputFiles)
            .withInterpreter(Property.ofValue(List.of("clickhouse-local")))
            .withBeforeCommands(this.beforeCommands)
            .withCommands(this.commands)
			.run();
	}

}
