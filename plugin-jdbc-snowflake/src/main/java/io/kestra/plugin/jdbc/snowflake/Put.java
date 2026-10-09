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

import java.nio.file.Path;
import java.net.URI;
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
    title = "Run a native Snowflake PUT command on an internal storage file",
    description = "Executes the native Snowflake PUT command on a file from Kestra internal storage. Use this task when native PUT options are required; use Upload for simpler file uploads to a Snowflake stage."
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

    @Schema(
        title = "Snowflake internal stage name",
        description = "Name of the Snowflake internal stage. The @ prefix is added automatically if omitted. External stages are not supported."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> stageName;

    @Schema(
        title = "Name of the file in the stage",
        description = "Defaults to the file name of the `from` URI."
    )
    @PluginProperty(group = "main")
    private Property<String> fileName;

    @Schema(
        title = "Whether to automatically compress the file"
    )
    @PluginProperty(group = "advanced")
    private Property<Boolean> autoCompress;

    @Schema(
        title = "Source file compression type"
    )
    @PluginProperty(group = "advanced")
    private Property<SourceCompression> sourceCompression;

    @Schema(
        title = "Whether to overwrite existing files"
    )
    @PluginProperty(group = "advanced")
    private Property<Boolean> overwrite;

    @Schema(
        title = "Parallelism",
        description = "Number of threads used for uploading. Must be between 1 and 99."
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> parallel;

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

    public enum SourceCompression {

        AUTO_DETECT,
        GZIP,
        BZ2,
        BROTLI,
        ZSTD,
        DEFLATE,
        RAW_DEFLATE,
        NONE
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rFrom = runContext.render(this.from)
            .as(String.class)
            .orElseThrow();

        String rStageName = runContext.render(this.stageName)
            .as(String.class)
            .orElseThrow();

        if (!rStageName.startsWith("@")) {
            rStageName = "@" + rStageName;
        }

        URI fromUri = new URI(rFrom);

        String rFileName = runContext.render(this.fileName)
            .as(String.class)
            .orElse(Path.of(fromUri.getPath()).getFileName().toString());

        if (rFileName.contains("*")
            || rFileName.contains("?")
            || rFileName.contains("'")) {
            throw new IllegalArgumentException(
                "fileName must not contain wildcard characters '*' or '?' or a single quote"
            );
        }

        var renderedParallel = runContext.render(this.parallel)
            .as(Integer.class);

        renderedParallel.ifPresent(value -> {
            if (value < 1 || value > 99) {
                throw new IllegalArgumentException(
                    "parallel must be between 1 and 99, got " + value
                );
            }
        });

        var tempFile = runContext.workingDir()
            .createFile(rFileName, runContext.storage().getFile(fromUri))
            .toFile();

        var rows = new ArrayList<Map<String, Object>>();

        try (
            Connection connection = this.connection(runContext);
            var statement = connection.createStatement()
        ) {
            StringBuilder sql = new StringBuilder(
                "PUT 'file://" + tempFile.getAbsolutePath() + "' " + rStageName
            );

            runContext.render(this.autoCompress)
                .as(Boolean.class)
                .ifPresent(value ->
                    sql.append(" AUTO_COMPRESS = ").append(value)
                );

            runContext.render(this.sourceCompression)
                .as(SourceCompression.class)
                .ifPresent(value ->
                    sql.append(" SOURCE_COMPRESSION = ").append(value.name())
                );

            runContext.render(this.overwrite)
                .as(Boolean.class)
                .ifPresent(value ->
                    sql.append(" OVERWRITE = ").append(value)
                );

            renderedParallel.ifPresent(value ->
                sql.append(" PARALLEL = ").append(value)
            );

            if (statement.execute(sql.toString())) {
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
