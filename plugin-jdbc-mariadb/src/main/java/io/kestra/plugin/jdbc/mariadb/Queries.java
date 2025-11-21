package io.kestra.plugin.jdbc.mariadb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.mariadb.jdbc.Driver;

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
    title = "Run multiple MariaDB queries."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a SQL query to a Mariadb Database and fetch a row as output.",
            full = true,
            code = """
                id: send_multiple_queries
                namespace: test.queries
                tasks:
                  - id: test_queries_insert
                    type: io.kestra.plugin.jdbc.mariadb.Queries
                    fetchType: FETCH
                    url: jdbc:mariadb://mariadb:3306/kestra
                    username: "${{secret('MARIADB_USERNAME')}}"
                    password: "${{secret('MARIADB_PASSWORD')}}"
                    sql: "{{ read('populate.sql') }}"

                  - id: test_queries_select
                    type: io.kestra.plugin.jdbc.mariadb.Queries
                    fetchType: FETCH
                    url: jdbc:mariadb://mariadb:3306/kestra
                    username: root
                    password: mariadb_passwd
                    sql: |
                      SELECT firstName, lastName FROM employee;
                      SELECT brand FROM laptop;
                """
        )
    },
    metrics = {
        @Metric(
            name = "fetch.size",
            type = Counter.TYPE,
            unit = "rows",
            description = "The number of fetched rows."
        )
    }
)
public class Queries extends AbstractJdbcQueries implements MariaDbConnectionInterface {

    @Schema(
        title = "Add input file to be loaded with `LOAD DATA LOCAL`.",
        description = "The file must be from Kestra's internal storage"
    )
    @PluginProperty(dynamic = true)
    protected String inputFile;

    @Getter(AccessLevel.NONE)
    protected transient Path workingDirectory;

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new MariaDbCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(Driver.class::isInstance)) {
            DriverManager.registerDriver(new Driver());
        }
    }

    @Override
    public Properties connectionProperties(RunContext runContext) throws Exception {
        return MariaDbUtils.createMariaDbProperties(super.connectionProperties(runContext), true);
    }

    @Override
    public Property<Integer> getFetchSize() {
        // The combination of useCursorFetch=true and preparedStatement.setFetchSize(10); push to use cursor on Mariadb DB instance side.
        // This leads to consuming DB instance disk memory when we try to fetch more than aware table size.
        // It actually just disables client-side caching of the entire response and gives you responses as they arrive as a result it has no effect on the DB
        return this.isStore() ? Property.ofValue(Integer.MIN_VALUE) : this.fetchSize;
    }

    @Override
    public AbstractJdbcQueries.MultiQueryOutput run(RunContext runContext) throws Exception {
        this.workingDirectory = runContext.workingDir().path();

        if (this.inputFile != null) {
            PluginUtilsService.createInputFiles(
                runContext,
                workingDirectory,
                Map.of("inputFile", this.inputFile),
                additionalVars
            );
        }

        additionalVars.put("inputFile", workingDirectory.toAbsolutePath().resolve("inputFile").toString());

        return super.run(runContext);
    }
}
