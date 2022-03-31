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
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Upload data to an internal stage"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "stageName: MYSTAGE",
                "prefix: testUploadStream",
                "fileName: destFile.csv"
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
        title = "The file to copy"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @Schema(
        title = "The stage name",
        description = "~ or table name or stage name"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String stageName;

    @Schema(
        title = "path / prefix under which the data should be uploaded on the stage"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String prefix;

    @Schema(
        title = "destination file name to use"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String fileName;

    @Schema(
        title = "compress data or not before uploading stream"
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
            title = "The url of the staged files"
        )
        private final URI uri;
    }
}
