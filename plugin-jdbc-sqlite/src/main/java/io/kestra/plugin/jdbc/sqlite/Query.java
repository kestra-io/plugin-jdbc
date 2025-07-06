package io.kestra.plugin.jdbc.sqlite;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcBaseQuery;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.sqlite.JDBC;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.*;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query a SQLite database."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Execute a query and pass the results to another task.",
            code = """
                id: sqlite_query
                namespace: company.team

                tasks:
                  - id: update
                    type: io.kestra.plugin.jdbc.sqlite.Query
                    url: jdbc:sqlite:myfile.db
                    sql: select concert_id, available, a, b, c, d, play_time, library_record, floatn_test, double_test, real_test, numeric_test, date_type, time_type, timez_type, timestamp_type, timestampz_type, interval_type, pay_by_quarter, schedule, json_type, blob_type from pgsql_types
                    fetchType: FETCH

                  - id: use_fetched_data
                    type: io.kestra.plugin.jdbc.sqlite.Query
                    url: jdbc:sqlite:myfile.db
                    sql: "{% for row in outputs.update.rows %} INSERT INTO pl_store_distribute (year_month,store_code, update_date) values ({{row.play_time}}, {{row.concert_id}}, TO_TIMESTAMP('{{row.timestamp_type}}', 'YYYY-MM-DDTHH:MI:SS.US') ); {% endfor %}"
                """
        ),
        @Example(
            full = true,
            title = "Execute a query, using existing sqlite file, and pass the results to another task.",
            code = """
                id: sqlite_query_using_file
                namespace: company.team

                tasks:
                  - id: update
                    type: io.kestra.plugin.jdbc.sqlite.Query
                    url: jdbc:sqlite:myfile.db
                    sqliteFile: {{ outputs.get.outputFiles['myfile.sqlite'] }}
                    sql: select * from pgsql_types
                    fetchType: FETCH

                  - id: use_fetched_data
                    type: io.kestra.plugin.jdbc.sqlite.Query
                    url: jdbc:sqlite:myfile.db
                    sqliteFile: {{ outputs.get.outputFiles['myfile.sqlite'] }}
                    sql: "{% for row in outputs.update.rows %} INSERT INTO pl_store_distribute (year_month,store_code, update_date) values ({{row.play_time}}, {{row.concert_id}}, TO_TIMESTAMP('{{row.timestamp_type}}', 'YYYY-MM-DDTHH:MI:SS.US') ); {% endfor %}"
                """
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, SqliteQueryInterface {
    @PluginProperty(group = "connection")
    protected Property<String> sqliteFile;

    @Builder.Default
    protected Property<Boolean> outputDbFile = Property.ofValue(false);

    @Getter(AccessLevel.NONE)
    protected transient Path workingDirectory;

    @Override
    public Properties connectionProperties(RunContext runContext) throws Exception {
        Properties properties = super.connectionProperties(runContext);
        return SqliteQueryUtils.buildSqliteProperties(properties, runContext);
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        Properties properties = super.connectionProperties(runContext);

        URI url = URI.create((String) properties.get("jdbc.url"));

        this.workingDirectory = runContext.workingDir().path();

        Optional<String> sqliteFileOptional = runContext.render(this.sqliteFile).as(String.class);
        String fileName = null;
        if (sqliteFileOptional.isPresent()) {
            // Get file name from url scheme parts, to be equally same as in connection url
            fileName = url.getSchemeSpecificPart().split(":")[1];

            PluginUtilsService.createInputFiles(
                runContext,
                workingDirectory,
                Map.of(fileName, sqliteFileOptional.get()),
                additionalVars
            );
        }

        AbstractJdbcBaseQuery.Output queryOutput = super.run(runContext);


        URI dbUri = null;
        if (Boolean.TRUE.equals(runContext.render(this.outputDbFile).as(Boolean.class).orElseThrow()) && sqliteFileOptional.isPresent()) {
            dbUri = runContext.storage().putFile(new File(workingDirectory.toString() + "/" + fileName), fileName);
        }

        return Output.builder()
            .databaseUri(dbUri)
            .row(queryOutput.getRow())
            .rows(queryOutput.getRows())
            .size(queryOutput.getSize())
            .uri(queryOutput.getUri())
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output extends AbstractJdbcQuery.Output {
        @Schema(
            title = "The database output URI in Kestra's internal storage."
        )
        @PluginProperty
        private final URI databaseUri;
    }

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new SqliteCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(JDBC.class::isInstance)) {
            DriverManager.registerDriver(new JDBC());
        }
    }

}
