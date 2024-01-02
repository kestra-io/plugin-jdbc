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
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
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
    title = "Download data to an internal stage."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "url: jdbc:snowflake://<account_identifier>.snowflakecomputing.com",
                "username: snowflake_user",
                "password: snowflake_passwd",
                "stageName: MYSTAGE",
                "fileName: prefix/destFile.csv"
            }
        )
    }
)
public class Download extends AbstractSnowflakeConnection implements RunnableTask<Download.Output> {
    private String database;
    private String warehouse;
    private String schema;
    private String role;

    @Schema(
        title = "The stage name.",
        description = "~ or table name or stage name."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String stageName;

    @Schema(
        title = "Destination file name to use."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String fileName;

    @Schema(
        title = "Whether to compress data before uploading stream."
    )
    @PluginProperty(dynamic = false)
    @NotNull
    @Builder.Default
    private Boolean compress = true;

    @Override
    public Download.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        File tempFile = runContext.tempFile().toFile();

        try (
            Connection conn = this.connection(runContext);
            BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(tempFile))
        ) {
            String stageName = runContext.render(this.stageName);
            String filename = runContext.render(this.fileName);

            logger.info("Starting download from stage '{}' with name '{}'", stageName, filename);

            InputStream inputStream = conn
                .unwrap(SnowflakeConnection.class)
                .downloadStream(
                    stageName,
                    filename,
                    this.compress
                );

            IOUtils.copyLarge(inputStream, outputStream);

            outputStream.flush();

            return Output
                .builder()
                .uri(runContext.putTempFile(tempFile))
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The URI of the file on Kestra internal storage."
        )
        private final URI uri;
    }
}
