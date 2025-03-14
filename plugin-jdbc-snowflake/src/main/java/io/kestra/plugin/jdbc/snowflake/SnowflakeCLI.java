package io.kestra.plugin.jdbc.snowflake;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.*;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.runner.docker.Docker;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.*;
import java.util.stream.Stream;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run snowflake commands."
)
@Plugin(
    examples = {
        @Example(
            title = "Show basic infos and connection status",
            full = true,
            code = {
                """
                    id: snowflake
                    namespace: company.team
                    tasks:
                      - id: list_stage_files
                        type: io.kestra.plugin.jdbc.snowflake.SnowflakeCLI
                        account: snowflake_account
                        username: snowflake_username
                        password: snowflake_password
                        commands:
                          - snow --info
                          - snow connection test
                    """
            }
        ),
        @Example(
            title = "List Snowflake staged files",
            full = true,
            code = {
                """
                    id: snowflake
                    namespace: company.team
                    tasks:
                      - id: list_stage_files
                        type: io.kestra.plugin.jdbc.snowflake.SnowflakeCLI
                        account: snowflake_account
                        username: snowflake_username
                        password: snowflake_password
                        commands:
                          - snow stage list-files @MY_WAREHOUSE.MY_SCHEMA.%MY_TABLE_STAGE_NAME
                    """
            }
        ),
        @Example(
            title = "Run Snowflake SQL select via CLI",
            full = true,
            code = {
                """
                    id: snowflake
                    namespace: company.team
                    tasks:
                      - id: query
                        type: io.kestra.plugin.jdbc.snowflake.SnowflakeCLI
                        account: snowflake_account
                        username: snowflake_username
                        password: snowflake_password
                        commands:
                          - snow sql --query="SELECT 1"
                    """
            }
        )
    }
)
public class SnowflakeCLI extends Task implements RunnableTask<ScriptOutput>, NamespaceFilesInterface, InputFilesInterface, OutputFilesInterface {

    public static final String DEFAULT_IMAGE = "ghcr.io/kestra-io/snowflake-cli";

    @Schema(
        title = "The commands to run before main list of commands."
    )
    protected Property<List<String>> beforeCommands;

    @Schema(
        title = "The commands to run. Please refer to SnowflakeCLI documentation https://docs.snowflake.com/en/developer-guide/snowflake-cli/command-reference/overview"
    )
    @NotNull
    protected Property<List<String>> commands;

    @Schema(
        title = "The account to use for authentication."
    )
    @NotNull
    protected Property<String> account;

    @Schema(
        title = "The username to use for authentication."
    )
    @NotNull
    protected Property<String> username;

    @Schema(
        title = "The password to use for authentication."
    )
    @NotNull
    protected Property<String> password;

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
        title = "The snowflake container image."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private String containerImage = DEFAULT_IMAGE;

    private NamespaceFiles namespaceFiles;

    private Object inputFiles;

    private Property<List<String>> outputFiles;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        var renderedOutputFiles = runContext.render(this.outputFiles).asList(String.class);
        var renderedCommands = runContext.render(this.commands).asList(String.class);
        var envVarsWithDefaultAuthentication = Optional.ofNullable(env).orElse(new HashMap<>());
        envVarsWithDefaultAuthentication.putAll(Map.of(
            "SNOWFLAKE_ACCOUNT", runContext.render(this.account).as(String.class).orElseThrow(),
            "SNOWFLAKE_USER", runContext.render(this.username).as(String.class).orElseThrow(),
            "SNOWFLAKE_PASSWORD", runContext.render(this.password).as(String.class).orElseThrow()
        ));

        return new CommandsWrapper(runContext)
            .withWarningOnStdErr(true)
            .withTaskRunner(this.taskRunner)
            .withContainerImage(this.containerImage)
            .withEnv(envVarsWithDefaultAuthentication)
            .withNamespaceFiles(namespaceFiles)
            .withInputFiles(inputFiles)
            .withOutputFiles(renderedOutputFiles.isEmpty() ? null : renderedOutputFiles)
            .withInterpreter(Property.of(List.of("/bin/sh", "-c")))
            .withBeforeCommands(this.beforeCommands)
            .withCommands(
                Property.of(
                    Stream.concat(
                        Stream.of(
                            "snow connection add --connection-name=default-connection --default --no-interactive"
                        ),
                        renderedCommands.stream()
                    ).toList()
                )
            )
            .run();
    }
}
