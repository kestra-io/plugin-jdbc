package org.kestra.task.jdbc;

import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.kestra.core.utils.Rethrow;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.sql.*;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Documentation(
    description = "Short description for this task",
    body = "Full description of this task"
)
public class JdbcQuery extends Task implements RunnableTask<JdbcQuery.Output> {
    @InputProperty(
        description = "Short description for this input",
        body = "Full description of this input",
        dynamic = true // If the variables will be rendered with template {{ }}
    )
    private String format;

    /*************************/
    @InputProperty(
        description = "The sql query to run",
        dynamic = true
    )
    private String sql;

    @Builder.Default
    @InputProperty(
        description = "Whether to Fetch only one data row from the query result to the task output",
        dynamic = false
    )
    private boolean fetchOne = false;

    @Builder.Default
    @InputProperty(
        description = "Whether to Fetch the data from the query result to the task output",
        dynamic = false
    )
    private boolean fetch = false;

    @InputProperty(
        description = "The jdbc url to connect to the database",
        dynamic = true
    )
    private String url;

    @InputProperty(
        description = "The database user",
        dynamic = true
    )
    private String username;

    @InputProperty(
        description = "The database user's password",
        dynamic = true
    )
    private String password;


    @Override
    public JdbcQuery.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        registerDrivers();

        Connection conn = DriverManager.getConnection(runContext.render(this.url), runContext.render(this.username), runContext.render(this.password));
        Statement stmt = conn.createStatement();

        try {

            boolean isResult = stmt.execute(runContext.render(this.sql));
            ResultSet rs = stmt.getResultSet();

            Output.OutputBuilder output = Output.builder();

            if (isResult) {
                if (this.fetchOne) {
                    output.row(fetchResult(stmt, rs));
                } else if (this.fetch) {
                    File tempFile = File.createTempFile(
                        this.getClass().getSimpleName().toLowerCase() + "_",
                        "." + FilenameUtils.getExtension("FETCH_FILE.ser") // FIXME : What format to use here ?
                    );

                    FileOutputStream fos = new FileOutputStream(tempFile);
                    ObjectOutputStream oos = new ObjectOutputStream(fos);

                    fetchResults(stmt, rs, oos);

                    output.uri(runContext.putTempFile(tempFile));
                }
            }

            return output.build();
        } finally {
            stmt.close();
        }
    }

    private Map<String, String> fetchResult(Statement stmt, ResultSet rs) throws SQLException {
        rs.next();
        return mapResultSetToMap(stmt, rs);
    }

    private int fetchResults(Statement stmt, ResultSet rs, ObjectOutputStream oos) throws SQLException, IOException {
        boolean isResult = false;
        int count = 0;

        do {
            while (rs.next()) {
                Map<String, String> map = mapResultSetToMap(stmt, rs);
                oos.writeObject(map);
                count++;
            }
            isResult = stmt.getMoreResults();
        } while (isResult);

        return count;
    }

    private Map<String, String> mapResultSetToMap(Statement stmt, ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsCount = rsmd.getColumnCount();

        return IntStream
            .range(1, columnsCount + 1)
            .boxed()
            .collect(Collectors.toMap(
                Rethrow.throwFunction(i -> rsmd.getColumnName(i)),
                // FIXME : No schema to use something else than string ?
                Rethrow.throwFunction(i -> rs.getString(i) != null ? rs.getString(i) : "")
            ));
    }

    private void registerDrivers() throws SQLException {
        // Mysql and Postgres are auto-registered thanks to the JSE Service Provider mechanism
        // See : https://docs.oracle.com/javase/8/docs/api/java/sql/DriverManager.html
        // and https://docs.oracle.com/javase/8/docs/technotes/guides/jar/jar.html#Service_Provider
        // TODO : Register Oracle Driver ?
        //Driver mysqlDriver = new com.mysql.jdbc.Driver();
        //Driver postgresqlDriver = new org.postgresql.Driver();
        //DriverManager.registerDriver(mysqlDriver);
        //DriverManager.registerDriver(postgresqlDriver);
    }

    /**
     * Input or Output can nested as you need
     */
    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {

        @OutputProperty(
            description = "Map containing the first row of fetched data",
            body = "Only populated if 'fetchOne' parameter is set to true."
        )
        private Map<String, String> row;

        @OutputProperty(
            description = "The url of the result file on kestra storage",
            body = "Only populated if 'fetch' parameter is set to true."
        )
        private URI uri;
    }
}
