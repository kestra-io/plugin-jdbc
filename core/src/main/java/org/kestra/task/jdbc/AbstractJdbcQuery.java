package org.kestra.task.jdbc;

import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.sql.*;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJdbcQuery extends Task {

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

    @InputProperty(
        description = "The time zone id to use for date/time manipulation. Defaut value is Z (zulu / UTC)",
        dynamic = false
    )
    private String timeZoneId;


    private AbstractCellConverter cellConverter;

    protected abstract AbstractCellConverter getCellConverter(ZoneId zoneId);

    /**
     * JDBC driver may be auto-registered. See https://docs.oracle.com/javase/8/docs/api/java/sql/DriverManager.html
     *
     * @throws SQLException
     */
    protected abstract void registerDriver() throws SQLException;


    public AbstractJdbcQuery.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        registerDriver();

        ZoneId zoneId = ZoneId.of("Z");
        if (this.timeZoneId != null) {
            zoneId = ZoneId.of(timeZoneId);
        }

        this.cellConverter = getCellConverter(zoneId);

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

    private List<Map<String, Object>> fetchResult(Statement stmt, ResultSet rs) throws SQLException {
        rs.next();
        return List.of(mapResultSetToMap(rs));
    }

    private int fetchResults(Statement stmt, ResultSet rs, ObjectOutputStream oos) throws SQLException, IOException {
        boolean isResult = false;
        int count = 0;

        do {
            while (rs.next()) {
                Map<String, Object> map = mapResultSetToMap(rs);
                oos.writeObject(map);
                count++;
            }
            isResult = stmt.getMoreResults();
        } while (isResult);

        return count;
    }

    private Map<String, Object> mapResultSetToMap(ResultSet rs) throws SQLException {
        int columnsCount = rs.getMetaData().getColumnCount();
        Map<String, Object> map = new HashMap<>();

        for (int i = 1; i <= columnsCount; i++) {
            map.put(rs.getMetaData().getColumnName(i), convertCell(i, rs));
        }

        return map;
    }

    private Object convertCell(int columnIndex, ResultSet rs) throws SQLException {
        return cellConverter.convertCell(columnIndex, rs);
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
        private List<Map<String, Object>> row;

        @OutputProperty(
            description = "The url of the result file on kestra storage",
            body = "Only populated if 'fetch' parameter is set to true."
        )
        private URI uri;
    }
}
