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
    title = "Run Snowflake CLI commands."
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
                      - id: log_info_and_connection_status
                        type: io.kestra.plugin.jdbc.snowflake.SnowflakeCLI
                        account: snowflake_account
                        username: "{{ secret('SNOWFLAKE_USERNAME') }}"
                        password: "{{ secret('SNOWFLAKE_PASSWORD') }}"
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
                        username: "{{ secret('SNOWFLAKE_USERNAME') }}"
                        password: "{{ secret('SNOWFLAKE_PASSWORD') }}"
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
                        username: "{{ secret('SNOWFLAKE_USERNAME') }}"
                        password: "{{ secret('SNOWFLAKE_PASSWORD') }}"
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
        title = "The commands to run. Please refer to SnowflakeCLI documentation https://docs.snowflake.com/en/developer-guide/snowflake-cli/command-reference/overview"
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<List<String>> commands;

    @Schema(
        title = "The account to use for authentication."
    )
    @PluginProperty(group = "main")
    @NotNull
    protected Property<String> account;

    @Schema(
        title = "The username to use for authentication."
    )
    @PluginProperty(group = "main")
    @NotNull
    protected Property<String> username;

    @Schema(
        title = "The password to use for authentication."
    )
    @PluginProperty(group = "connection")
    protected Property<String> password;

    @Schema(
        title = "The private key file for key pair authentication and key rotation for authentication/",
        description = "It needs to be an un-encoded private key in plaintext like: 'MIIEvwIBADA...EwKx0TSWT9A=='"
    )
    @PluginProperty(group = "connection")
    protected Property<String> privateKey;

    @Schema(
        title = "Specifies the private key password for key pair authentication and key rotation."
    )
    @PluginProperty(group = "connection")
    protected Property<String> privateKeyPassword;

    @Schema(
        title = "Additional environment variables for the current process."
    )
    @PluginProperty(dynamic = true, group = "execution")
    protected Map<String, String> env;

    @Schema(
        title = "The task runner to use."
    )
    @Valid
    @PluginProperty(group = "execution")
    @Builder.Default
    private TaskRunner<?> taskRunner = Docker.instance();

    @Schema(
        title = "The snowflake container image."
    )
    @PluginProperty(dynamic = true, group = "execution")
    @Builder.Default
    private String containerImage = DEFAULT_IMAGE;

    @PluginProperty(group = "source")
    private NamespaceFiles namespaceFiles;

    @PluginProperty(group = "source")
    private Object inputFiles;

    @PluginProperty(group = "destination")
    private Property<List<String>> outputFiles;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        var rOutputFiles = runContext.render(this.outputFiles).asList(String.class);
        var rCommands = runContext.render(this.commands).asList(String.class);
        var rAccount = runContext.render(this.account).as(String.class).orElseThrow();
        var rUsername = runContext.render(this.username).as(String.class).orElseThrow();
        var envVarsWithDefaultAuthentication = Optional.ofNullable(env).orElse(new HashMap<>());
        envVarsWithDefaultAuthentication.putAll(Map.of(
            "SNOWFLAKE_ACCOUNT", rAccount,
            "SNOWFLAKE_USER", rUsername

        ));
        if (this.password != null) {
            var rPassword = runContext.render(this.password).as(String.class).orElseThrow();
            envVarsWithDefaultAuthentication.putAll(Map.of(
                "SNOWFLAKE_PASSWORD", rPassword
            ));
        }
        if (this.privateKey != null) {
            var rPrivateKey = runContext.render(this.privateKey).as(String.class).orElseThrow();
            var rPrivateKeyPassword = runContext.render(this.privateKeyPassword).as(String.class);
            var tempPrivateKeyFile = runContext
                .workingDir().createTempFile(
                    formatPrivateKeyToPEM(
                        Base64.getEncoder().encodeToString(
                            RSAKeyPairUtils.deserializePrivateKey(
                                rPrivateKey,
                                rPrivateKeyPassword
                            ).getEncoded()
                        )
                    ).getBytes()
                );
            envVarsWithDefaultAuthentication.putAll(Map.of(
                "SNOWFLAKE_AUTHENTICATOR", "SNOWFLAKE_JWT",
                "SNOWFLAKE_PRIVATE_KEY_PATH", tempPrivateKeyFile.toAbsolutePath().toString()
            ));
        }

        return new CommandsWrapper(runContext)
            .withWarningOnStdErr(true)
            .withTaskRunner(this.taskRunner)
            .withContainerImage(this.containerImage)
            .withEnv(envVarsWithDefaultAuthentication)
            .withNamespaceFiles(namespaceFiles)
            .withInputFiles(inputFiles)
            .withOutputFiles(rOutputFiles.isEmpty() ? null : rOutputFiles)
            .withInterpreter(Property.ofValue(List.of("/bin/sh", "-c")))
            .withCommands(
                Property.ofValue(
                    Stream.concat(
                        Stream.of(
                            "snow connection add --connection-name=default-connection --default --no-interactive"
                        ),
                        rCommands.stream()
                    ).toList()
                )
            )
            .run();
    }

    private String formatPrivateKeyToPEM(String privateKey) {
        if (privateKey.contains("-----BEGIN")) {
            return privateKey;
        }
        return "-----BEGIN PRIVATE KEY-----\n" + privateKey + "\n-----END PRIVATE KEY-----";
    }
}
