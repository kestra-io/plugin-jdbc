package io.kestra.plugin.jdbc.snowflake;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import net.snowflake.client.jdbc.SnowflakeConnection;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.InputStream;
import java.net.URI;
import java.sql.Connection;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Upload data to an internal stage. Make sure that the `stageName` follows the naming convention of `@databaseName.schemaName.%stageOrTableName`. For usage examples, check the Blueprints tagged with `Snowflake`."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "url: jdbc:snowflake://<account_identifier>.snowflakecomputing.com",
                "username: snowflake_user",
                "password: snowflake_passwd",
                "from: '{{ outputs.extract.uri }}'",
                "fileName: data.csv",
                "prefix: raw",
                "stageName: @demo_db.public.%mytable",
            }
        )
    }
)
public class Upload extends AbstractSnowflakeConnection implements RunnableTask<Upload.Output> {
    private String database;
    private String warehouse;
    private String schema;
    private String role;

    @Schema(
        title = "Path to the file to load to internal stage in Snowflake"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @Schema(
        title = "The stage name",
        description = "This can either be a stage name or a table name"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String stageName;

    @Schema(
        title = "The prefix under which the file will be uploaded to the internal stage"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String prefix;

    @Schema(
        title = "Destination file name to use"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String fileName;

    @Schema(
        title = "Whether to compress the file or not before uploading it to the internal stage"
    )
    @PluginProperty(dynamic = false)
    @NotNull
    @Builder.Default
    private Boolean compress = true;

    @Override
    public Upload.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        URI from = new URI(runContext.render(this.from));
        try (
            Connection conn = this.connection(runContext);
            InputStream inputStream = runContext.uriToInputStream(from);
        ) {
            String stageName = runContext.render(this.stageName);
            String prefix = runContext.render(this.prefix);
            String filename = runContext.render(this.fileName);

            logger.info("Starting upload to stage '{}' on '{}' with name '{}'", stageName, prefix, filename);

            conn
                .unwrap(SnowflakeConnection.class)
                .uploadStream(
                    stageName,
                    prefix,
                    inputStream,
                    filename,
                    this.compress
                );

            return Output
                .builder()
                .uri(URI.create(StringUtils.stripEnd(prefix, "/") + "/" + filename + (this.compress ? ".gz" : "")))
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The URL of the staged files"
        )
        private final URI uri;
    }
}
