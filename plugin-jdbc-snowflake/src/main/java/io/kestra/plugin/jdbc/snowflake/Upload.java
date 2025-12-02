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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.InputStream;
import java.net.URI;
import java.sql.Connection;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Upload data from an internal storage file to a Snowflake stage.",
    description = "Make sure that the `stageName` follows the naming convention of `@databaseName.schemaName.%stageOrTableName`. For usage examples, check the Blueprints tagged with `Snowflake`."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                   id: snowflake_upload
                   namespace: company.team

                   tasks:
                     - id: upload
                       type: io.kestra.plugin.jdbc.snowflake.Upload
                       url: jdbc:snowflake://<account_identifier>.snowflakecomputing.com
                       username: "{{ secret('SNOWFLAKE_USERNAME') }}"
                       password: "{{ secret('SNOWFLAKE_PASSWORD') }}"
                       from: '{{ outputs.extract.uri }}'
                       fileName: data.csv
                       prefix: raw
                       stageName: "@demo_db.public.%myStage"
                   """
        )
    }
)
public class Upload extends AbstractSnowflakeConnection implements RunnableTask<Upload.Output> {

    @PluginProperty(group = "connection")
    private Property<String> database;

    @PluginProperty(group = "connection")
    private Property<String> warehouse;

    @PluginProperty(group = "connection")
    private Property<String> schema;

    @PluginProperty(group = "connection")
    private Property<String> role;

    @PluginProperty(dynamic = true)
    private Property<String> queryTag;

    @Schema(
        title = "Path to the file to load to Snowflake stage."
    )
    @NotNull
    private Property<String> from;

    @Schema(
        title = "Snowflake stage name.",
        description = "This can either be a stage name or a table name."
    )
    @NotNull
    private Property<String> stageName;

    @Schema(
        title = "The prefix under which the file will be uploaded to Snowflake stage."
    )
    @NotNull
    private Property<String> prefix;

    @Schema(
        title = "Destination file name to use."
    )
    @NotNull
    private Property<String> fileName;

    @Schema(
        title = "Whether to compress the file or not before uploading it to the Snowflake stage."
    )
    @NotNull
    @Builder.Default
    private Property<Boolean> compress = Property.ofValue(true);

    @Override
    public Upload.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());
        try (
            Connection conn = this.connection(runContext);
            InputStream inputStream = runContext.storage().getFile(from);
        ) {
            String stageName = runContext.render(this.stageName).as(String.class).orElseThrow();
            String prefix = runContext.render(this.prefix).as(String.class).orElseThrow();
            String filename = runContext.render(this.fileName).as(String.class).orElseThrow();

            logger.info("Starting upload to stage '{}' on '{}' with name '{}'", stageName, prefix, filename);

            conn
                .unwrap(SnowflakeConnection.class)
                .uploadStream(
                    stageName,
                    prefix,
                    inputStream,
                    filename,
                    runContext.render(this.compress).as(Boolean.class).orElseThrow()
                );

            return Output
                .builder()
                .uri(URI.create(StringUtils.stripEnd(prefix, "/") + "/" + filename + (runContext.render(this.compress).as(Boolean.class).orElseThrow() ? ".gz" : "")))
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The URI of the staged file."
        )
        private final URI uri;
    }
}
