package io.kestra.plugin.jdbc.mysql;

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
    title = "Query a MySQL database."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a SQL query to a MySQL Database and fetch a row as output.",
            full = true,
            code = """
                id: mysql_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.jdbc.mysql.Query
                    url: jdbc:mysql://127.0.0.1:3306/
                    username: mysql_user
                    password: mysql_password
                    sql: select * from mysql_types
                    fetchType: FETCH_ONE
                """
        ),
        @Example(
            title = "Load a csv file into a MySQL table.",
            full = true,
            code = """
                id: mysql_query
                namespace: company.team

                tasks:
                  - id: http_download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/csv/products.csv

                  - id: query
                    type: io.kestra.plugin.jdbc.mysql.Query
                    url: jdbc:mysql://127.0.0.1:3306/
                    username: mysql_user
                    password: mysql_password
                    inputFile: "{{ outputs.http_download.uri }}"
                    sql: |
                      LOAD DATA LOCAL INFILE '{{ inputFile }}'
                      INTO TABLE products
                      FIELDS TERMINATED BY ','
                      ENCLOSED BY '"'
                      LINES TERMINATED BY '\\n'
                      IGNORE 1 ROWS;
                """
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, AutoCommitInterface {
    protected final Property<Boolean> autoCommit = Property.of(true);

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
        return new MysqlCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new com.mysql.cj.jdbc.Driver());
    }

    @Override
    public Properties connectionProperties(RunContext runContext) throws Exception {
        return MysqlUtils.createMysqlProperties(super.connectionProperties(runContext), this.workingDirectory, false);
    }

    @Override
    public Property<Integer> getFetchSize() {
        // The combination of useCursorFetch=true and preparedStatement.setFetchSize(10); push to use cursor on MySql DB instance side.
        // This leads to consuming DB instance disk memory when we try to fetch more than aware table size.
        // It actually just disables client-side caching of the entire response and gives you responses as they arrive as a result it has no effect on the DB
        return this.isStore() ? Property.of(Integer.MIN_VALUE) : this.fetchSize;
    }

    @Override
    public AbstractJdbcQuery.Output run(RunContext runContext) throws Exception {
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
