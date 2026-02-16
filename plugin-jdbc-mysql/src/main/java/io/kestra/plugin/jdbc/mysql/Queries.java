package io.kestra.plugin.jdbc.mysql;

import com.mysql.cj.jdbc.Driver;
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
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

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
    title = "Execute multiple SQL statements against MySQL",
    description = "Runs multiple SQL statements separated by semicolons. To enable multi-statement execution, add `allowMultiQueries=true` to the JDBC URL. Supports parameterized queries, transactions (default enabled), and all fetch modes. For STORE mode, uses streaming to minimize memory. Supports LOAD DATA LOCAL INFILE via inputFile property."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a SQL query to a MySQL Database and fetch a row as output.",
            full = true,
            code = """
                id: send_multiple_queries
                namespace: test.queries
                tasks:
                  - id: test_queries_insert
                    type: io.kestra.plugin.jdbc.mysql.Queries
                    fetchType: FETCH
                    url: jdbc:mysql://mysql:3306/kestra?allowMultiQueries=true
                    username: "${{secret('MYSQL_USERNAME')}}"
                    password: "${{secret('MYSQL_PASSWORD')}}"
                    sql: "{{ read('populate.sql') }}"

                  - id: test_queries_select
                    type: io.kestra.plugin.jdbc.mysql.Queries
                    fetchType: FETCH
                    url: jdbc:mysql://mysql:3306/kestra
                    username: root
                    password: mysql_passwd
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
public class Queries extends AbstractJdbcQueries implements MySqlConnectionInterface {

    @Schema(
        title = "Input file for LOAD DATA LOCAL INFILE operations",
        description = "URI to a file in Kestra's internal storage (kestra://). Used with MySQL's LOAD DATA LOCAL INFILE statement to efficiently load CSV or delimited files into tables"
    )
    @PluginProperty(dynamic = true)
    protected String inputFile;

    @Getter(AccessLevel.NONE)
    protected transient Path workingDirectory;

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new MysqlCellConverter(zoneId);
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
        return this.createMysqlProperties(super.connectionProperties(runContext), this.workingDirectory, true);
    }

    @Override
    protected Integer getFetchSize(RunContext runContext) throws IllegalVariableEvaluationException {
        // The combination of useCursorFetch=true and preparedStatement.setFetchSize(10); push to use cursor on the MySQL instance side.
        // This leads to consuming DB instance disk memory when we try to fetch more than aware table size.
        // It actually just disables client-side caching of the entire response and gives you responses as they arrive as a result it has no effect on the DB
        return this.renderFetchType(runContext) == FetchType.STORE ?
            Integer.MIN_VALUE : runContext.render(this.fetchSize).as(Integer.class).orElse(Integer.MIN_VALUE);
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
