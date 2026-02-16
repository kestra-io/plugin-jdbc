package io.kestra.plugin.jdbc.mariadb;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
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
    title = "Execute multiple SQL statements against MariaDB",
    description = "Runs multiple SQL statements separated by semicolons. To enable multi-statement execution, add `allowMultiQueries=true` to the JDBC URL. Supports parameterized queries, transactions (default enabled), and all fetch modes. Default fetchSize is 10,000 rows. Supports LOAD DATA LOCAL INFILE via inputFile property."
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
                    url: jdbc:mariadb://mariadb:3306/kestra?allowMultiQueries=true
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
        title = "Input file for LOAD DATA LOCAL INFILE operations",
        description = "URI to a file in Kestra's internal storage (kestra://). Used with MariaDB's LOAD DATA LOCAL INFILE statement to efficiently load CSV or delimited files into tables"
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
    protected Integer getFetchSize(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.fetchSize).as(Integer.class).orElse(10000);
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
