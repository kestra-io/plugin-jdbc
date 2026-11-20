package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.JdbcConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;
import java.util.Properties;

public interface DuckDbQueryInterface extends JdbcConnectionInterface {
    List<String> DEFAULT_COMMUNITY_EXTENSIONS = List.of("ion");

    @Schema(
        title = "Input files to be loaded from DuckDb",
        description = "Describe a files map that will be written and usable by DuckDb. " +
            "You can reach files by their filename, example: `SELECT * FROM read_csv_auto('myfile.csv');` " +
            "Relative paths are resolved by DuckDB against the Kestra process directory, not the task working directory; " +
            "the working directory is only used as a fallback for reads when no file with that name exists in the process directory. " +
            "If such a file exists, the task fails to avoid silently reading the wrong data. " +
            "To be unambiguous, reference the file as `'{{ workingDir }}/myfile.csv'`."
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
            Relative paths in `COPY ... TO` are written by DuckDB to the Kestra process directory, not the working directory, so they are not captured: use `{{ workingDir }}/...` or `{{ outputFiles.key }}`.
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
        description = """
            Kestra's URI to an existing DuckDB database file. DuckDB 2.0 can read and update 1.x files, preserving their storage format. \
            New databases use the DuckDB 2.0 storage format by default; set `storageVersion` to target an older reader for newly created files.
            """
    )
    @PluginProperty(group = "advanced")
    Property<String> getDatabaseUri();

    @Schema(
        title = "Output the database file",
        description = """
            Upload the database file to internal storage for further processing. New databases use the DuckDB 2.0 storage format by default, \
            which DuckDB 1.x cannot open. Set `storageVersion`, for example `v1.5.6`, before creating a file for older consumers. \
            A database loaded via databaseUri retains its existing format; this setting does not downgrade an existing file.
            """
    )
    @PluginProperty(group = "advanced")
    Property<Boolean> getOutputDbFile();

    @Schema(
        title = "Storage compatibility version",
        description = """
            Optional DuckDB storage compatibility target, such as `v1.5.6` or `latest`. Applied at connection startup before a new database file is created. \
            Set `v1.5.6` when the exported database must remain readable by DuckDB 1.5.6. DuckDB validates supported version strings. \
            When omitted, new files use DuckDB 2.0's default storage format; existing files retain their format. This property does not convert existing files, \
            restore older SQL behavior, or enable MotherDuck compatibility. Features that cannot be stored in the selected format may fail.
            """,
        example = "v1.5.6"
    )
    @PluginProperty(group = "advanced")
    Property<String> getStorageVersion();

    @Schema(
        title = "DuckDB community extensions to install and load before running the SQL",
        description = """
            Defaults to `["ion"]`. On supported Linux platforms, bundled extensions are installed from a task-local repository \
            pinned with DuckDB's community public keys, keeping signature validation enabled. Repository changes are forbidden after bundled installation. \
            Bundled installation uses a temporary task-local cache; Kestra restores DuckDB's worker cache before user SQL runs. \
            Community and autoloaded extensions retain the worker cache, preserving pre-provisioned extensions and reuse across tasks. \
            Otherwise Kestra uses `INSTALL <ext> FROM community` followed by `LOAD <ext>`. If bundled loading fails, the community install is attempted. \
            If installation or loading fails, Kestra logs a warning and continues.
            """
    )
    @PluginProperty(group = "advanced")
    Property<List<String>> getCommunityExtensions();

    @Override
    default Properties connectionProperties(RunContext runContext) throws Exception {
        var properties = JdbcConnectionInterface.super.connectionProperties(runContext);
        if (getStorageVersion() != null) {
            var rStorageVersion = runContext.render(getStorageVersion()).as(String.class).orElse("");
            if (rStorageVersion.isBlank()) {
                throw new IllegalArgumentException("storageVersion must resolve to a DuckDB version, such as v1.5.6, or latest");
            }
            properties.setProperty("storage_compatibility_version", rStorageVersion);
        }
        DuckDbConnectionSetup.configureConnectionProperties(properties, runContext.workingDir().path());
        return properties;
    }

    @Override
    default String getScheme() {
        return "jdbc:duckdb";
    }
}
