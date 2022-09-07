package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.tasks.scripts.BashService;
import io.kestra.plugin.jdbc.AutoCommitInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwBiConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query a local DuckDb"
)
@Plugin(
    examples = {
        @Example(
            title = "Execute a query that read a csv and output another one",
            code = {
                "url: 'jdbc:duckdb:'",
                "timeZoneId: Europe/Paris",
                "sql: |-",
                "  CREATE TABLE new_tbl AS SELECT * FROM read_csv_auto('{{workingDir}}/in.csv', header=True);",
                "",
                "  COPY (SELECT id, name FROM new_tbl) TO '{{ outputFiles.out }}' (HEADER, DELIMITER ',');",
                "inputFiles:",
                "  in.csv: {{ inputs.csv }}",
                "outputFiles:",
                "- out"
            }
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<Query.Output>, AutoCommitInterface {
    protected final Boolean autoCommit = true;

    @Schema(
        title = "Input files to be loaded from DuckDb.",
        description = "Describe a files map that will be written and usable by DuckDb. " +
            "You can reach files using a `workingDir` variable, example: `SELECT * FROM read_csv_auto('{{workingDir}}/myfile.csv');` "
    )
    @PluginProperty(
        additionalProperties = String.class,
        dynamic = true
    )
    protected Map<String, String> inputFiles;

    @Schema(
        title = "Output file list that will be uploaded to internal storage",
        description = "List of key that will generate temporary files.\n" +
            "On the sql query, just can use with special variable named `outputFiles.key`.\n" +
            "If you add a files with `[\"first\"]`, you can use the special vars `COPY tbl TO '{[ outputFiles.first }}' (HEADER, DELIMITER ',');`" +
            " and you used on others tasks using `{{ outputs.task-id.files.first }}`"
    )
    @PluginProperty(dynamic = false)
    protected List<String> outputFiles;

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new DuckDbCellConverter(zoneId);
    }

    @Override
    protected void registerDriver() throws SQLException {
        DriverManager.registerDriver(new org.duckdb.DuckDBDriver());
    }

    @Override
    public Query.Output run(RunContext runContext) throws Exception {
        Map<String, String> outputFiles = null;

        Path databaseFile = runContext.tempFile();
        Files.delete(databaseFile);

        this.url = "jdbc:duckdb:" + databaseFile;
        additionalVars.put("workingDir", runContext.tempDir().toAbsolutePath().toString());

        // inputFiles
        BashService.createInputFiles(
            runContext,
            runContext.tempDir(),
            this.inputFiles,
            additionalVars
        );

        // outputFiles
        if (this.outputFiles != null && this.outputFiles.size() > 0) {
            outputFiles = BashService.createOutputFiles(
                runContext.tempDir(),
                this.outputFiles,
                additionalVars
            );
        }

        AbstractJdbcQuery.Output run = super.run(runContext);

        // upload output files
        Map<String, URI> uploaded = new HashMap<>();

        if (outputFiles != null) {
            outputFiles
                .forEach(throwBiConsumer((k, v) -> uploaded.put(k, runContext.putTempFile(new File(runContext.render(v, additionalVars))))));
        }

        return Output.builder()
            .row(run.getRow())
            .rows(run.getRows())
            .uri(run.getUri())
            .size(run.getSize())
            .outputFiles(uploaded)
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output extends AbstractJdbcQuery.Output {
        @Schema(
            title = "The output files uri in Kestra internal storage"
        )
        @PluginProperty(additionalProperties = URI.class)
        private final Map<String, URI> outputFiles;
    }
}
