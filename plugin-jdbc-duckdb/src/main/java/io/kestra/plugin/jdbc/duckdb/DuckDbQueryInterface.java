package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

public interface DuckDbQueryInterface {
    @Schema(
        title = "Input files to be loaded from DuckDb.",
        description = "Describe a files map that will be written and usable by DuckDb. " +
            "You can reach files by their filename, example: `SELECT * FROM read_csv_auto('myfile.csv');` "
    )
    @PluginProperty(
        additionalProperties = String.class,
        dynamic = true
    )
    Object getInputFiles();

    @Schema(
        title = "Output file list that will be uploaded to internal storage.",
        description = "List of keys that will generate temporary files.\n" +
            "On the SQL query, you can just use a variable named `outputFiles.key` for the corresponding file.\n" +
            "If you add a file with `[\"first\"]`, you can use the special vars `COPY tbl TO '{{ outputFiles.first }}' (HEADER, DELIMITER ',');`" +
            " and use this file in others tasks using `{{ outputs.taskId.outputFiles.first }}`."
    )
    Property<List<String>> getOutputFiles();

    @Schema(
        title = "Database URI",
        description = "Kestra's URI to an existing Duck DB database file"
    )
    Property<String> getDatabaseUri();

    @Schema(
        title = "Output the database file.",
        description = "This property lets you define if you want to output the in-memory database as a file for further processing."
    )
    Property<Boolean> getOutputDbFile();
}
