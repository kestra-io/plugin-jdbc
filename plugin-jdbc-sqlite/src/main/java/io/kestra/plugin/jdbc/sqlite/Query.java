package io.kestra.plugin.jdbc.sqlite;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AutoCommitInterface;
import io.micronaut.http.uri.UriBuilder;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.sqlite.JDBC;

import java.net.URI;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query a SQLite server."
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
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, AutoCommitInterface {
    protected final Property<Boolean> autoCommit = Property.of(true);

    @Schema(
        title = "Add sqlite file.",
        description = "The file must be from Kestra's internal storage"
    )
    @PluginProperty(dynamic = true)
    protected String sqliteFile;

    @Getter(AccessLevel.NONE)
    protected transient Path workingDirectory;

    @Override
    public Properties connectionProperties(RunContext runContext) throws Exception {
        Properties properties = super.connectionProperties(runContext);

        URI url = URI.create((String) properties.get("jdbc.url"));

        // get file name from url scheme parts
        String filename = url.getSchemeSpecificPart().split(":")[1];

        Path path = runContext.workingDir().resolve(Path.of(filename));
        if (path.toFile().exists()) {
            url = URI.create(path.toString());

            UriBuilder builder = UriBuilder.of(url);

            builder.scheme("jdbc:sqlite");

            properties.put("jdbc.url", builder.build().toString());
        }

        return properties;
    }

    @Override
    public AbstractJdbcQuery.Output run(RunContext runContext) throws Exception {
        Properties properties = super.connectionProperties(runContext);

        URI url = URI.create((String) properties.get("jdbc.url"));

        this.workingDirectory = runContext.workingDir().path();

        if (this.sqliteFile != null) {

            // Get file name from url scheme parts, to be equally same as in connection url
            String filename = url.getSchemeSpecificPart().split(":")[1];

            PluginUtilsService.createInputFiles(
                runContext,
                workingDirectory,
                Map.of(filename, this.sqliteFile),
                additionalVars
            );
        }

        return super.run(runContext);
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
