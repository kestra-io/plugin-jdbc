package io.kestra.plugin.jdbc.mysql;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.tasks.scripts.BashService;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "MySQL Query Task.",
    description = "Currently supported types are the following ones : \n" +
        " - serial,\n" +
        " - tinyint,\n" +
        " - char(n),\n" +
        " - varchar(n),\n" +
        " - text,\n" +
        " - bigint,\n" +
        " - bit(n),\n" +
        " - float,\n" +
        " - double,\n" +
        " - numeric,\n" +
        " - decimal,\n" +
        " - date,\n" +
        " - datetime(n),\n" +
        " - time,\n" +
        " - timestamp(n),\n" +
        " - year(n),\n" +
        " - json,\n" +
        " - blob"
)
@Plugin(
    examples = {
        @Example(
            title = "Request a MySQL Database and fetch a row as outputs",
            code = {
                "url: jdbc:mysql://127.0.0.1:56982/",
                "username: mysql_user",
                "password: mysql_passwd",
                "sql: select * from mysql_types",
                "fetchOne: true",
            }
        ),
        @Example(
            title = "Load a csv file into a MySQL table",
            code = {
                "url: jdbc:mysql://127.0.0.1:56982/?allowLoadLocalInfile=true",
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
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output> {
    @Schema(
        title = "Add input file to be loaded with `LOAD DATA LOCAL`.",
        description = "The file must be from Kestra internal storage"
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
    protected void registerDriver() throws SQLException {
        DriverManager.registerDriver(new com.mysql.cj.jdbc.Driver());
    }

    @Override
    public AbstractJdbcQuery.Output run(RunContext runContext) throws Exception {
        this.workingDirectory = runContext.tempDir();

        if (this.inputFile != null) {
            BashService.createInputFiles(
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
