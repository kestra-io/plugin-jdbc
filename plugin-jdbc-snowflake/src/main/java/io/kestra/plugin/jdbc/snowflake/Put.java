package io.kestra.plugin.jdbc.snowflake;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Upload data from an internal storage file to a Snowflake stage",
    description = "Executes the native Snowflake PUT command to upload a file from Kestra internal storage to a Snowflake stage."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: snowflake_put
                namespace: company.team

                tasks:
                  - id: put
                    type: io.kestra.plugin.jdbc.snowflake.Put
                    url: jdbc:snowflake://<account_identifier>.snowflakecomputing.com
                    username: "{{ secret('SNOWFLAKE_USERNAME') }}"
                    password: "{{ secret('SNOWFLAKE_PASSWORD') }}"
                    from: '{{ outputs.extract.uri }}'
                    stageName: "@demo_db.public.%myStage"
                """
        )
    }
)
public class Put extends AbstractSnowflakeConnection implements RunnableTask<Put.Output> {

    @Schema(title = "Path to the file to upload to Snowflake")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> from;

    @Schema(title = "Snowflake stage name")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> stageName;

    @PluginProperty(group = "connection")
    private Property<String> database;

    @PluginProperty(group = "advanced")
    private Property<String> warehouse;

    @PluginProperty(group = "connection")
    private Property<String> schema;

    @PluginProperty(group = "advanced")
    private Property<String> role;

    @PluginProperty(group = "advanced")
    private Property<String> queryTag;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rFrom = runContext.render(this.from)
            .as(String.class)
            .orElseThrow();

        String rStageName = runContext.render(this.stageName)
            .as(String.class)
            .orElseThrow();

        URI fromUri = new URI(rFrom);

        var tempFile = runContext.workingDir()
            .createTempFile()
            .toFile();

        try (InputStream inputStream = runContext.storage().getFile(fromUri)) {
            Files.copy(
                inputStream,
                tempFile.toPath(),
                StandardCopyOption.REPLACE_EXISTING
            );
        }

        var rows = new ArrayList<Map<String, Object>>();

        try (
            Connection connection = this.connection(runContext);
            var statement = connection.createStatement()
        ) {
            String sql = "PUT 'file://" + tempFile.getAbsolutePath() + "' " + rStageName;

            if (statement.execute(sql)) {
                try (var resultSet = statement.getResultSet()) {
                    var metadata = resultSet.getMetaData();
                    int columnCount = metadata.getColumnCount();

                    while (resultSet.next()) {
                        var row = new LinkedHashMap<String, Object>();

                        for (int i = 1; i <= columnCount; i++) {
                            row.put(
                                metadata.getColumnLabel(i),
                                resultSet.getObject(i)
                            );
                        }

                        rows.add(row);
                    }
                }
            }
        }

        return Output.builder()
            .rows(rows)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "Snowflake PUT result")
        private final List<Map<String, Object>> rows;
    }
}