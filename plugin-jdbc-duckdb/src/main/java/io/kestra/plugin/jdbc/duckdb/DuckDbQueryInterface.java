package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.plugin.jdbc.JdbcConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

public interface DuckDbQueryInterface extends JdbcConnectionInterface {
    List<String> DEFAULT_COMMUNITY_EXTENSIONS = List.of("ion");

    @Schema(
        title = "Input files to be loaded from DuckDb",
        description = "Describe a files map that will be written and usable by DuckDb. " +
            "You can reach files by their filename, example: `SELECT * FROM read_csv_auto('myfile.csv');` "
    )
    @PluginProperty(group = "source", 
        additionalProperties = String.class,
        dynamic = true
    )
    Object getInputFiles();

    @Deprecated
    @Schema(
        title = "Output file list that will be uploaded to internal storage",
        description = """
            Deprecated: Files generated within the query in the working directory are now automatically captured as output files.
            List of keys that will generate temporary files.
            On the SQL query, you can just use a variable named `outputFiles.key` for the corresponding file.
            If you add a file with `["first"]`, you can use the special vars `COPY tbl TO '{{ outputFiles.first }}' (HEADER, DELIMITER ',');` and use this file in other tasks using `{{ outputs.taskId.outputFiles.first }}`.
            For files captured automatically with extensions or directory paths (e.g., `results.csv`), use bracket syntax: `{{ outputs.taskId.outputFiles['results.csv'] }}`.""",
        deprecated = true
    )
    @PluginProperty(group = "destination")
    Property<List<String>> getOutputFiles();

    @Schema(
        title = "Whether to automatically capture output files",
        description = """
            Whether to automatically capture and upload files created in the working directory during execution. Defaults to true.
            Note that files ending in `.db`, `.wal`, or `.tmp`, the task's database file, the `.duckdb_extensions/` directory,
            and pre-existing unchanged files are excluded from automatic capture."""
    )
    @PluginProperty(group = "destination")
    default Property<Boolean> getCaptureOutputFiles() {
        return Property.ofValue(true);
    }

    @Schema(
        title = "Maximum number of output files to automatically capture",
        description = "Maximum number of new or modified files to automatically capture and upload. If exceeded, the task will fail. Defaults to 100."
    )
    @PluginProperty(group = "destination")
    default Property<Integer> getMaxCapturedFiles() {
        return Property.ofValue(100);
    }

    @Schema(
        title = "Maximum total size (in bytes) of output files to automatically capture",
        description = "Maximum combined size in bytes of new or modified files to automatically capture and upload. If exceeded, the task will fail. Defaults to 104857600 (100 MB)."
    )
    @PluginProperty(group = "destination")
    default Property<Long> getMaxCapturedBytes() {
        return Property.ofValue(100L * 1024 * 1024);
    }

    @Schema(
        title = "Database URI",
        description = "Kestra's URI to an existing Duck DB database file"
    )
    @PluginProperty(group = "advanced")
    Property<String> getDatabaseUri();

    @Schema(
        title = "Output the database file",
        description = "This property lets you define if you want to output the in-memory database as a file for further processing."
    )
    @PluginProperty(group = "advanced")
    Property<Boolean> getOutputDbFile();

    @Schema(
        title = "DuckDB community extensions to install and load before running the SQL",
        description = "Defaults to `[\"ion\"]`. Each extension is attempted on a best-effort basis using `INSTALL <ext> FROM community` followed by `LOAD <ext>`. If installation or loading fails, Kestra logs a warning and continues."
    )
    @PluginProperty(group = "advanced")
    Property<List<String>> getCommunityExtensions();

    @Override
    default String getScheme() {
        return "jdbc:duckdb";
    }

    // DuckDB uses per-execution in-memory or file-scoped connections; pooling is not safe here.
    @Override
    default boolean usesConnectionPool() {
        return false;
    }
}
