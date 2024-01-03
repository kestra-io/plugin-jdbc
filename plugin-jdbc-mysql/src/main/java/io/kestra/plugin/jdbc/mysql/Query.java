package io.kestra.plugin.jdbc.mysql;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.tasks.PluginUtilsService;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AutoCommitInterface;
import io.micronaut.http.uri.UriBuilder;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

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
    title = "Query a MySQL database."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a sql query to a MySQL Database and fetch a row as output.",
            code = {
                "url: jdbc:mysql://127.0.0.1:3306/",
                "username: mysql_user",
                "password: mysql_passwd",
                "sql: select * from mysql_types",
                "fetchOne: true",
            }
        ),
        @Example(
            title = "Load a csv file into a MySQL table.",
            code = {
                "url: jdbc:mysql://127.0.0.1:3306/",
                "username: mysql_user",
                "password: mysql_passwd",
                "inputFile: \"{{ outputs.taskId.file }}\"",
                "sql: |",
                "  LOAD DATA LOCAL INFILE '{{ inputFile }}'",
                "  INTO TABLE discounts" +
                "  FIELDS TERMINATED BY ','",
                "  ENCLOSED BY '\"'",
                "  LINES TERMINATED BY '\\n'",
                "  IGNORE 1 ROWS;",
            }
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, AutoCommitInterface {
    protected final Boolean autoCommit = true;

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
        Properties props = super.connectionProperties(runContext);

        URI url = URI.create((String) props.get("jdbc.url"));
        url = URI.create(url.getSchemeSpecificPart());

        UriBuilder builder = UriBuilder.of(url);

        // allow local in file for current worker and prevent the global one
        builder.queryParam("allowLoadLocalInfileInPath", this.workingDirectory.toAbsolutePath().toString());
        builder.replaceQueryParam("allowLoadLocalInfile", false);

        // see https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-implementation-notes.html
        // By default, ResultSets are completely retrieved and stored in memory.
        builder.replaceQueryParam("useCursorFetch", true);

        builder.scheme("jdbc:mysql");

        props.put("jdbc.url", builder.build().toString());

        return props;
    }

    public Integer getFetchSize() {
        // The combination of useCursorFetch=true and preparedStatement.setFetchSize(10); push to use cursor on MySql DB instance side.
        // This leads to consuming DB instance disk memory when we try to fetch more than aware table size.
        // It actually just disables client-side caching of the entire response and gives you responses as they arrive as a result it has no effect on the DB
        return this.isStore() ? Integer.MIN_VALUE : this.fetchSize;
    }

    @Override
    public AbstractJdbcQuery.Output run(RunContext runContext) throws Exception {
        this.workingDirectory = runContext.tempDir();

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
