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
import net.snowflake.client.jdbc.SnowflakeConnection;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.sql.Connection;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Download data from Snowflake stage to Kestra's internal storage."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                   id: snowflake_download
                   namespace: company.team

                   tasks:
                     - id: download
                       type: io.kestra.plugin.jdbc.snowflake.Download
                       url: jdbc:snowflake://<account_identifier>.snowflakecomputing.com
                       username: snowflake_user
                       password: snowflake_password
                       stageName: "@demo_db.public.%myStage"
                       fileName: prefix/destFile.csv
                   """
        )
    }
)
public class Download extends AbstractSnowflakeConnection implements RunnableTask<Download.Output> {
    private Property<String> database;
    private Property<String> warehouse;
    private Property<String> schema;
    private Property<String> role;

    @Schema(
        title = "Snowflake stage name.",
        description = "~ or table name or stage name."
    )
    @NotNull
    private Property<String> stageName;

    @Schema(
        title = "File name on Snowflake stage that should be downloaded."
    )
    @NotNull
    private Property<String> fileName;

    @Schema(
        title = "Whether to compress data before uploading stream."
    )
    @NotNull
    @Builder.Default
    private Property<Boolean> compress = Property.of(true);

    @Override
    public Download.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        File tempFile = runContext.workingDir().createTempFile().toFile();

        try (
            Connection conn = this.connection(runContext);
            BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(tempFile))
        ) {
            String stageName = runContext.render(this.stageName).as(String.class).orElseThrow();
            String filename = runContext.render(this.fileName).as(String.class).orElseThrow();

            logger.info("Starting download from stage '{}' with name '{}'", stageName, filename);

            InputStream inputStream = conn
                .unwrap(SnowflakeConnection.class)
                .downloadStream(
                    stageName,
                    filename,
                    runContext.render(this.compress).as(Boolean.class).orElseThrow()
                );

            IOUtils.copyLarge(inputStream, outputStream);

            outputStream.flush();

            return Output
                .builder()
                .uri(runContext.storage().putFile(tempFile))
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The URI of the file on Kestra's internal storage."
        )
        private final URI uri;
    }
}
